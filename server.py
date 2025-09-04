import os, asyncio, re, math, logging, random, time, hashlib
from datetime import timezone, datetime, timedelta
from typing import Optional, Iterable, List, Tuple

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import asyncpg
import json

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError, UsernameNotOccupiedError, UsernameInvalidError
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel, Message, MessageService
import aiohttp  # оставил на будущее для авто-сидера (можно удалить, если не нужен)

logging.basicConfig(level=logging.INFO)

# ============================== ENV ==============================
API_ID  = int(os.getenv("API_ID","0"))
API_HASH = os.getenv("API_HASH","")

# анти-повторы: TTL в часах
ANTI_REPEAT_TTL_HOURS = float(os.getenv("ANTI_REPEAT_TTL_HOURS","12"))
GLOBAL_USER_KEY = "__GLOBAL__"
GLOBAL_QHASH_ALL = "__ANY__"

def load_sessions_from_env() -> list[str]:
    sessions: list[str] = []

    raw_json = os.getenv("TELEGRAM_STRING_SESSIONS_JSON", "")
    if raw_json.strip():
        try:
            arr = json.loads(raw_json)
            if isinstance(arr, list):
                sessions += [str(s).strip() for s in arr if str(s).strip()]
        except Exception as e:
            logging.warning(f"Bad TELEGRAM_STRING_SESSIONS_JSON: {e}")

    raw = os.getenv("TELEGRAM_STRING_SESSIONS", "")
    if raw.strip():
        for part in re.split(r"[\n,;|]+", raw):
            part = part.strip()
            if part:
                sessions.append(part)

    for k, v in os.environ.items():
        if k.startswith("TELEGRAM_SESSION_") or k.startswith("TG_SESSION_"):
            v = (v or "").strip()
            if v:
                sessions.append(v)

    single = os.getenv("TELEGRAM_STRING_SESSION", "").strip()
    if single:
        sessions.append(single)

    return list(dict.fromkeys([s for s in sessions if s]))

SESS_LIST = load_sessions_from_env()
if not SESS_LIST:
    raise RuntimeError("No TELEGRAM sessions provided (check TELEGRAM_* env vars)")

logging.info(f"Loaded {len(SESS_LIST)} Telegram sessions")

PG_DSN = os.getenv("DATABASE_URL","postgresql://postgres:postgres@localhost:5432/postgres")

RUN_CRAWLER = os.getenv("RUN_CRAWLER","0").lower() in ("1","true","yes")
LIVE_MAX_CHATS = int(os.getenv("LIVE_MAX_CHATS", "120"))
LIVE_PER_CHANNEL_LIMIT = int(os.getenv("LIVE_PER_CHANNEL_LIMIT", "220"))
LIVE_KNOWN_PER_CHANNEL = int(os.getenv("LIVE_KNOWN_PER_CHANNEL", "200"))
CRAWL_QUEUE_BATCH = int(os.getenv("CRAWL_QUEUE_BATCH", "120"))
MAX_PARALLEL_CHANNELS = int(os.getenv("MAX_PARALLEL_CHANNELS", "6"))
CRAWLER_SLEEP_SECONDS = int(os.getenv("CRAWLER_SLEEP_SECONDS", "120"))
AUTO_SEED_ENABLED = os.getenv("AUTO_SEED_ENABLED", "0").lower() in ("1", "true", "yes")  # ← фикс: убрал лишнюю скобку
AUTO_SEED_SOURCES = os.getenv("AUTO_SEED_SOURCES", "https://tgstat.com/ru/top-channels,https://tgstat.com/ru/categories/news,https://tgstat.com/ru/categories/technology,https://tgstat.com/ru/categories/entertainment,https://tgstat.com/ru/categories/business")
AUTO_SEED_INTERVAL_SEC = int(os.getenv("AUTO_SEED_INTERVAL_SEC", "21600"))
AUTO_SEED_PER_URL_DELAY = float(os.getenv("AUTO_SEED_PER_URL_DELAY", "0.7"))
AUTO_SEED_FETCH_TIMEOUT = int(os.getenv("AUTO_SEED_FETCH_TIMEOUT", "20"))
AUTO_SEED_MAX_BYTES = int(os.getenv("AUTO_SEED_MAX_BYTES", "5242880"))

app = FastAPI()
_db_pool: asyncpg.Pool | None = None

_flood_block_until: float = 0.0

# ============================== DB DDL ==============================
DDL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TABLE IF NOT EXISTS channels(
  chat_id BIGINT PRIMARY KEY,
  username TEXT,
  title TEXT,
  type TEXT
);

CREATE TABLE IF NOT EXISTS messages(
  chat_id BIGINT,
  message_id BIGINT,
  date TIMESTAMPTZ,
  text TEXT,
  has_media BOOLEAN,
  links INT,
  views INT,
  forwards INT,
  PRIMARY KEY(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS served_hits(
  user_key TEXT,
  qhash TEXT,
  chat_id BIGINT,
  message_id BIGINT,
  ts TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY(user_key, qhash, chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS crawl_queue(
  username TEXT PRIMARY KEY,
  last_try TIMESTAMPTZ,
  tries INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_msg_tsv_ru ON messages
USING GIN (to_tsvector('russian', coalesce(text,'')));

CREATE INDEX IF NOT EXISTS ix_msg_tsv_en ON messages
USING GIN (to_tsvector('english', coalesce(text,'')));

CREATE INDEX IF NOT EXISTS ix_msg_date ON messages(date DESC);

CREATE INDEX IF NOT EXISTS ix_msg_trgm ON messages
USING GIN ((lower(coalesce(text,''))) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS ix_chan_name_ru ON channels
USING GIN (to_tsvector('russian', coalesce(title,'')));

CREATE TABLE IF NOT EXISTS external_usernames (
  username   TEXT PRIMARY KEY,
  sources    TEXT[] DEFAULT '{}',
  first_seen TIMESTAMPTZ DEFAULT NOW(),
  last_seen  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_external_last_seen ON external_usernames(last_seen DESC);
CREATE INDEX IF NOT EXISTS ix_external_sources_gin ON external_usernames USING GIN (sources);
"""

async def db():
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(
            dsn=PG_DSN,
            min_size=0,
            max_size=5,
            max_inactive_connection_lifetime=30.0,
            timeout=30.0,
        )
    async with _db_pool.acquire() as c:
        await c.execute(DDL)
    return _db_pool

# ============================== Helpers ==============================
TG_USERNAME_RE = re.compile(r"(?:^|[\s:;/\(\)\[\]\.,])@([a-zA-Z0-9_]{4,})\b|https?://t\.me/([a-zA-Z0-9_]{4,})\b")

def _flood_blocked() -> bool:
    return time.time() < _flood_block_until

def _set_flood_block(seconds: int):
    global _flood_block_until
    _flood_block_until = time.time() + max(5, seconds)

def flood_left() -> int:
    return int(max(0, _flood_block_until - time.time()))

class AllSessionsFlooded(Exception):
    def __init__(self, seconds: Optional[int] = None):
        self.seconds = seconds
        super().__init__(f"All sessions got FloodWait ({seconds}s)")

def _is_valid_uname(u: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_]{4,32}", u or ""))

async def push_usernames_to_queue(conn, usernames: List[str]):
    if not usernames: return
    usernames = [u.lower() for u in usernames if _is_valid_uname(u)]
    if not usernames: return
    await conn.executemany(
        "INSERT INTO crawl_queue(username, last_try, tries) VALUES($1, NULL, 0) ON CONFLICT DO NOTHING",
        [(u,) for u in set(usernames)]
    )

async def upsert_external_usernames(conn, usernames: List[str], source: str) -> int:
    if not usernames: return 0
    rows = [(u, source) for u in usernames if _is_valid_uname(u)]
    if not rows: return 0
    await conn.executemany("""
        INSERT INTO external_usernames(username, sources)
        VALUES($1, ARRAY[$2]::text[])
        ON CONFLICT (username) DO UPDATE
        SET last_seen = NOW(),
            sources = CASE
                WHEN external_usernames.sources @> ARRAY[$2]::text[] THEN external_usernames.sources
                ELSE array_append(external_usernames.sources, $2)
            END
    """, rows)
    return len(rows)

def extract_usernames_from_text(txt: str) -> List[str]:
    found = []
    for m in TG_USERNAME_RE.finditer(txt or ""):
        u = (m.group(1) or m.group(2) or "").lower()
        if u and _is_valid_uname(u):
            found.append(u)
    return list(dict.fromkeys(found))

def extract_usernames_from_any_text(raw: str) -> List[str]:
    if not raw: return []
    found = set()
    pat_links = re.compile(r"(?:@|https?://t\.me/)([A-Za-z0-9_]{4,32})", re.I)
    found |= {m.group(1).lower() for m in pat_links.finditer(raw)}
    simple = {x.lower() for x in re.findall(r"\b[A-Za-z0-9_]{4,32}\b", raw)}
    blacklist = {"joinchat","addstickers","socks","iv","share","proxy"}
    found |= (simple - blacklist)
    return [u for u in found if _is_valid_uname(u)]

async def drop_bad_usernames(conn, usernames: List[str]):
    if not usernames: return
    await conn.executemany("DELETE FROM crawl_queue WHERE username=$1", [(u,) for u in set(usernames)])

# ============================== Sessions ==============================
class SessionPool:
    def __init__(self, sessions: List[str]):
        self.sessions = [s for s in sessions if s]
        self.idx = 0
        if not self.sessions:
            raise RuntimeError("No TELEGRAM sessions provided")

    def next_session(self) -> StringSession:
        s = self.sessions[self.idx % len(self.sessions)]
        self.idx += 1
        logging.info(f"Using TG session #{(self.idx - 1) % len(self.sessions) + 1}")
        return StringSession(s)

    async def client(self) -> TelegramClient:
        return TelegramClient(self.next_session(), API_ID, API_HASH, flood_sleep_threshold=0)

SESS_POOL = SessionPool(SESS_LIST)

# ============================== Flood-aware resolve ==============================
_BACKOFF_BY_SESSION: dict[int, float] = {}
_MIN_ROTATION_DELAY = float(os.getenv("MIN_ROTATION_DELAY","0.3"))
_JITTER_MAX = float(os.getenv("ROTATION_JITTER_MAX","0.4"))
_PER_SESSION_COOLDOWN = float(os.getenv("PER_SESSION_COOLDOWN","8"))

async def resolve_entity_with_rotation(uname: str, tries: Optional[int] = None):
    max_tries = tries or len(SESS_LIST)
    last_wait: Optional[int] = None

    for i in range(max_tries):
        sess_idx = SESS_POOL.idx % len(SESS_LIST)
        now = time.time()
        next_ok = _BACKOFF_BY_SESSION.get(sess_idx, 0.0)
        if now < next_ok:
            await asyncio.sleep(min(1.5, next_ok - now))

        cli = await SESS_POOL.client()
        try:
            await asyncio.sleep(_MIN_ROTATION_DELAY + random.random() * _JITTER_MAX)
            async with cli:
                ent = await cli.get_entity(uname)
                return ent

        except FloodWaitError as e:
            sec = int(getattr(e, "seconds", 30) or 30)
            last_wait = sec
            _set_flood_block(sec)
            _BACKOFF_BY_SESSION[sess_idx] = time.time() + max(_PER_SESSION_COOLDOWN, min(sec, 180))
            logging.warning(f"Flood on @{uname} (try {i+1}/{max_tries}); block={sec}s; session={sess_idx+1}")
            continue
        except (UsernameNotOccupiedError, UsernameInvalidError, ValueError):
            raise
        except RPCError as e:
            logging.info(f"RPC error on @{uname}: {e!r}, rotate")
            continue

    raise AllSessionsFlooded(last_wait)

# ============================== Live backfill ==============================
async def live_backfill_messages(cli: TelegramClient, conn, channels: list[Channel], days: int, per_channel: int = 250):
    since_dt = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    sem = asyncio.Semaphore(MAX_PARALLEL_CHANNELS)
    new_channels = 0
    new_messages = 0

    async def _one(ch: Channel):
        nonlocal new_channels, new_messages
        async with sem:
            try:
                ent = await cli.get_entity(ch)
            except RPCError:
                return
            created = await upsert_channel(
                conn,
                ent.id,
                getattr(ent, "username", None),
                getattr(ent, "title", None),
                "channel" if getattr(ent, "broadcast", False) else "supergroup",
            )
            if created:
                new_channels += 1

            async for msg in cli.iter_messages(ent, limit=per_channel):
                if msg.date and msg.date.tzinfo and msg.date < since_dt:
                    break
                dt = msg.date if msg.date else datetime.now(timezone.utc)
                row = {
                    "chat_id": ent.id,
                    "message_id": msg.id,
                    "date": dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc),
                    "text": msg.message or "" if not isinstance(msg, MessageService) else "",
                    "has_media": bool(getattr(msg, "media", None)),
                    "links": len(re.findall(r"https?://", msg.message or "")),
                    "views": getattr(msg, "views", 0) or 0,
                    "forwards": getattr(msg, "forwards", 0) or 0,
                }
                if await upsert_message(conn, row):
                    new_messages += 1
                if row["text"]:
                    us = extract_usernames_from_text(row["text"])
                    if us:
                        await push_usernames_to_queue(conn, us)

    await asyncio.gather(*[_one(ch) for ch in channels], return_exceptions=True)
    return new_channels, new_messages

# ============================== Crawl queue ==============================
async def crawl_from_queue(per_channel: int = 300, batch: int = CRAWL_QUEUE_BATCH) -> Tuple[int,int,int]:
    if _flood_blocked():
        logging.info(f"Crawler: queue skipped; left={flood_left()}s")
        return (0,0,0)

    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT username FROM crawl_queue
            ORDER BY COALESCE(last_try, to_timestamp(0)) ASC
            LIMIT $1
        """, batch)
        if not rows:
            return (0,0,0)

        usernames = [r["username"] for r in rows]
        await conn.executemany(
            "UPDATE crawl_queue SET last_try=NOW(), tries=COALESCE(tries,0)+1 WHERE username=$1",
            [(u,) for u in usernames]
        )

        known = await conn.fetch("SELECT lower(username) AS u FROM channels WHERE username = ANY($1::text[])", usernames)
        known_set = {r["u"] for r in known if r["u"]}
        to_resolve = [u for u in usernames if u and u.lower() not in known_set]
        if known_set:
            await conn.executemany("DELETE FROM crawl_queue WHERE username=$1", [(u,) for u in known_set])

        try:
            chans: List[Channel] = []
            bad: List[str] = []
            not_channel = 0

            for uname in to_resolve:
                if not _is_valid_uname(uname):
                    bad.append(uname); continue
                try:
                    ent = await resolve_entity_with_rotation(uname)
                    if isinstance(ent, Channel) and getattr(ent, "username", None):
                        chans.append(ent)
                    else:
                        not_channel += 1; bad.append(uname)
                except (UsernameNotOccupiedError, UsernameInvalidError, ValueError):
                    bad.append(uname)
                except AllSessionsFlooded as e:
                    logging.warning(f"All sessions flooded on @{uname}; block={e.seconds or flood_left()}s")
                    break
                except FloodWaitError as e:
                    _set_flood_block(int(getattr(e,"seconds",30) or 30))
                    logging.warning(f"Crawler: FloodWait on @{uname}")
                    break

            ch_new = 0; msg_new = 0
            if chans:
                async with (await SESS_POOL.client()) as cli:
                    ch_new, msg_new = await live_backfill_messages(cli, conn, chans, days=365, per_channel=per_channel)

            if bad: await drop_bad_usernames(conn, bad)

            logging.info("Crawler: queue processed=%d, to_resolve=%d, known=%d, not_channel=%d, bad=%d, new_channels=%d, new_messages=%d",
                         len(usernames), len(to_resolve), len(known_set), not_channel, len(bad), ch_new, msg_new)
            return (ch_new, msg_new, len(usernames))

        except FloodWaitError as e:
            _set_flood_block(int(getattr(e,"seconds",30) or 30))
            logging.warning("Crawler: FloodWait outside loop")
            return (0,0,0)

# ============================== Seeds ==============================
CASINO_BRANDS = [
    "1xbet","pin-up","pinnacle","joycasino","vulkan","parimatch","fonbet","leonbets",
    "bet365","ggbet","melbet","betfair","betway","winline","mostbet","888casino",
    "pokerstars","partypoker","unibet","22bet","mr green","stake","roobet","cloudbet","bitstarz",
]
CASINO_RU = ["казино","онлайн казино","фриспины","бездеп","бонус казино","слоты","игровые автоматы","рулетка","покер","беттинг","букмекер","ставки на спорт"]
CASINO_EN = ["casino","gambling","free spins","no deposit","bonus code","slots","roulette","blackjack","poker","betting","bookmaker"]

AFF_NETWORKS = ["cpa","affiliate","revshare","hybrid deal","adsterra","propellerads","leadbit","maxbounty","awempire","everad"]

START_USERNAMES: List[str] = []

GENERAL_WIDE_SEEDS = ["marketing","seo","smm","арбитраж трафика","промокод","скидки","купоны","заработок в интернете","контент маркетинг"]

SEEDS_ALL = list(dict.fromkeys(CASINO_BRANDS + AFF_NETWORKS + CASINO_RU + CASINO_EN + GENERAL_WIDE_SEEDS))

# ============================== Query helpers ==============================
SYNONYMS = {
    r"\bфри[-\s]?спин(ы|ов|а)?\b": ["free spins","фриспин","спины","бесплатные вращения"],
    r"\bбездеп(озит(ный|а)?)?\b": ["без депозита","no deposit","бездепозитный","no-deposit"],
    r"\bбонус(ы|ов)?\b": ["bonus","промокод","promo code","cashback","кэшбек","вейджер","wager"],
    r"\bказино\b": ["casino","онлайн казино","slots","слоты","игровые автоматы"],
}

PROMO_POS = re.compile(r"(бонус|free\s*spins?|фри[-\s]?спин|промо-?код|промокод|бездеп|депозит\s*бонус|welcome|фриспин|cash\s*back|кэшбек)", re.I)
PROMO_NEG = re.compile(r"(мем|шутк|юмор|сарказм|ирони|мемы|прикол)", re.I)

# азартные слова/бренды
GAMBLING_WORDS = list(dict.fromkeys(CASINO_BRANDS + CASINO_RU + CASINO_EN + ["freebet","букмекер","bet","slot"]))
GAMBLING_RE = re.compile("|".join([re.escape(w) for w in sorted(GAMBLING_WORDS, key=len, reverse=True)]), re.I)

SPAM_NEG = re.compile(
    r"(free\s*movies?|tv\s*shows?|stream|прям(ая|ые)\s*трансляц|расписани[ея]|schedule|fixtures|"
    r"live\s*score|match|vs\s|лига|серия\s*a|серия\s*b|кубак?|epl|la\s*liga|bundesliga|"
    r"прогноз(ы)?\s*на\s*матч|коэфф(ициент)?|odds|parlay)",
    re.I
)

def expand_query(q: str) -> str:
    ql = q.lower()
    extra = []
    for pat, syns in SYNONYMS.items():
        if re.search(pat, ql): extra += syns
    return (q + (" " + " ".join(sorted(set(extra))) if extra else ""))

def is_promotional(text: str) -> bool:
    if not text: return False
    t = text.lower()
    return bool(PROMO_POS.search(t)) and not bool(PROMO_NEG.search(t))

def is_spammy(text: str) -> bool:
    if not text: return False
    return bool(SPAM_NEG.search(text.lower()))

def recency_weight(date: datetime, now: datetime) -> float:
    age_days = max(0.0, (now - date).total_seconds() / 86400.0)
    return math.exp(-age_days / 30.0)

def log1p(x: int | float) -> float:
    try: return math.log1p(max(0.0, float(x)))
    except Exception: return 0.0

def tokens_for_like(q: str) -> list[str]:
    toks = re.findall(r"\w+", q.lower())
    return [t for t in toks if len(t) >= 3][:10]

def normalize_query_for_hash(q: str) -> str:
    q = q.lower().strip()
    q = re.sub(r"\s+", " ", q)
    return q

def qhash(q: str) -> str:
    return hashlib.md5(normalize_query_for_hash(q).encode("utf-8")).hexdigest()

# ---------- Intent detection ----------
def detect_intent(q: str):
    ql = q.lower()
    def anyw(words): return any(w in ql for w in words)

    # казино — строго
    if anyw(["casino","казино","фриспин","free spins","бездеп","промокод","слоты","slots","ставки","беттинг"]):
        return dict(
            name="casino_promo",
            include=GAMBLING_WORDS,
            exclude=[],
            strict=True,
            boost=0.35,
            penalty=0.15,
            sim_threshold=0.22
        )

    # вакансии
    if anyw(["ваканс","vacancy","job","работа","hiring"]):
        return dict(name="jobs", include=["вакан","vacancy","job","работ"], exclude=[], strict=False, boost=0.25, penalty=0.0, sim_threshold=0.26)

    # generic: жестко исключаем азартку и поднимаем порог похожести
    return dict(
        name="generic",
        include=[],
        exclude=GAMBLING_WORDS,
        strict=True,
        boost=0.0,
        penalty=0.0,
        sim_threshold=0.30
    )

# ============================== Live helpers ==============================
def make_links(username: Optional[str], message_id: Optional[int]):
    if not username:
        return None, None
    ch = f"https://t.me/{username}"
    msg = f"{ch}/{message_id}" if message_id else None
    return ch, msg

def channels_only(chats: Iterable) -> list[Channel]:
    return [c for c in chats if isinstance(c, Channel) and getattr(c, "username", None)]

# ============================== Upserts ==============================
async def upsert_channel(conn, chat_id, username, title, typ) -> bool:
    inserted = await conn.fetchval("""
        INSERT INTO channels(chat_id, username, title, type)
        VALUES($1,$2,$3,$4)
        ON CONFLICT (chat_id) DO NOTHING
        RETURNING 1
    """, chat_id, username, title, typ)
    if not inserted:
        await conn.execute("UPDATE channels SET username=$2, title=$3, type=$4 WHERE chat_id=$1",
                           chat_id, username, title, typ)
        return False
    return True

async def upsert_message(conn, row) -> bool:
    inserted = await conn.fetchval("""
        INSERT INTO messages(chat_id, message_id, date, text, has_media, links, views, forwards)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (chat_id, message_id) DO NOTHING
        RETURNING 1
    """, row["chat_id"], row["message_id"], row["date"], row["text"], row["has_media"],
         row["links"], row["views"], row["forwards"])
    return bool(inserted)

# ============================== Live search ==============================
async def live_find_channels(cli: TelegramClient, query: str, limit: int = 80) -> list[Channel]:
    if _flood_blocked(): return []
    try:
        await asyncio.sleep(0.15 + random.random()*0.25)
        res = await cli(SearchRequest(q=query, limit=limit))
        return channels_only(res.chats)
    except FloodWaitError as e:
        _set_flood_block(int(getattr(e,"seconds",30) or 30)); return []
    except RPCError:
        pass
    try:
        other = await SESS_POOL.client()
        async with other:
            await asyncio.sleep(0.2 + random.random()*0.3)
            res = await other(SearchRequest(q=query, limit=limit))
            return channels_only(res.chats)
    except FloodWaitError as e:
        _set_flood_block(int(getattr(e,"seconds",30) or 30)); return []
    except RPCError:
        return []

# ============================== Crawler (periodic) ==============================
async def crawl_once(seeds: list[str], per_channel: int = 500) -> Tuple[int,int,int]:
    if _flood_blocked():
        logging.info(f"Crawler: seeds skipped; left={flood_left()}s")
        return (0,0,0)
    pool = await db()
    async with pool.acquire() as conn:
        try:
            async with (await SESS_POOL.client()) as cli:
                seeded = list(dict.fromkeys(seeds + SEEDS_ALL))
                random.shuffle(seeded)
                total_new_ch = 0; total_new_msg = 0; queries_run = 0
                for q in seeded:
                    chans = await live_find_channels(cli, q, limit=min(LIVE_MAX_CHATS,120))
                    queries_run += 1
                    if not chans: continue
                    ch_new, msg_new = await live_backfill_messages(cli, conn, chans, days=365, per_channel=per_channel)
                    total_new_ch += ch_new; total_new_msg += msg_new
                return (total_new_ch, total_new_msg, queries_run)
        except FloodWaitError as e:
            _set_flood_block(e.seconds); return (0,0,0)

async def crawler_loop():
    base_sleep = max(10, CRAWLER_SLEEP_SECONDS)
    sleep_now = base_sleep
    max_sleep = max(base_sleep, int(os.getenv("CRAWLER_MAX_SLEEP_SECONDS","1800")))

    while True:
        try:
            if _flood_blocked():
                left = flood_left()
                logging.info(f"Crawler: FloodWait; sleeping {left}s")
                await asyncio.sleep(max(5, left))
                sleep_now = min(sleep_now*2, max_sleep)
                continue

            ch_add_q, msg_add_q, processed = await crawl_from_queue(per_channel=350, batch=CRAWL_QUEUE_BATCH)
            logging.info(f"Crawler: from queue processed={processed}, new_channels={ch_add_q}, new_messages={msg_add_q}")

            ch_add_s, msg_add_s, qrun = await crawl_once(SEEDS_ALL, per_channel=320)
            logging.info(f"Crawler: seeds queries_run={qrun}, new_channels={ch_add_s}, new_messages={msg_add_s}")

            added_total = (ch_add_q + ch_add_s) + (msg_add_q + msg_add_s)
            if added_total == 0 and processed == 0 and qrun == 0:
                sleep_now = min(sleep_now*2, max_sleep)
                logging.info(f"Crawler: no progress, backoff sleep={sleep_now}s")
            else:
                sleep_now = base_sleep

        except Exception as e:
            logging.error(f"Crawler error: {e}")
            sleep_now = min(max(30, int(sleep_now*1.5)), max_sleep)

        await asyncio.sleep(sleep_now)

# ============================== Anti-repeat ==============================
async def _filter_and_record_antirepeat(conn, user_key: Optional[str], q: str, items: List[dict]) -> Tuple[int, List[dict]]:
    ttl = max(1.0, ANTI_REPEAT_TTL_HOURS)
    now_limit = datetime.now(timezone.utc) - timedelta(hours=ttl)

    recent = await conn.fetch("""
        SELECT chat_id, message_id
        FROM served_hits
        WHERE user_key=$1 AND qhash=$2 AND ts >= $3
    """, GLOBAL_USER_KEY, GLOBAL_QHASH_ALL, now_limit)
    recent_set = {(int(r["chat_id"]), int(r["message_id"])) for r in recent}

    user_recent_set = set()
    if user_key:
        h = qhash(q)
        ur = await conn.fetch("""
            SELECT chat_id, message_id
            FROM served_hits
            WHERE user_key=$1 AND qhash=$2 AND ts >= $3
        """, user_key, h, now_limit)
        user_recent_set = {(int(r["chat_id"]), int(r["message_id"])) for r in ur}

    filt: List[dict] = []
    for it in items:
        cid = it.get("chat_id"); mid = it.get("message_id")
        if cid and mid:
            pair = (int(cid), int(mid))
            if pair in recent_set or pair in user_recent_set:
                continue
        filt.append(it)

    rows_global = [(GLOBAL_USER_KEY, GLOBAL_QHASH_ALL, it.get("chat_id"), it.get("message_id"))
                   for it in filt if it.get("chat_id") and it.get("message_id")]
    if rows_global:
        await conn.executemany("""
            INSERT INTO served_hits(user_key, qhash, chat_id, message_id)
            VALUES($1,$2,$3,$4)
        """, rows_global)

    if user_key:
        h = qhash(q)
        rows_user = [(user_key, h, it.get("chat_id"), it.get("message_id"))
                     for it in filt if it.get("chat_id") and it.get("message_id")]
        if rows_user:
            await conn.executemany("""
                INSERT INTO served_hits(user_key, qhash, chat_id, message_id)
                VALUES($1,$2,$3,$4)
            """, rows_user)

    return len(items), filt

def _diversify(items: list[dict], max_per_channel: int = 2) -> list[dict]:
    seen = {}
    out = []
    for it in items:
        ch = it.get("chat_username") or it.get("chat")
        c = seen.get(ch, 0)
        if c < max_per_channel:
            out.append(it); seen[ch] = c + 1
    return out

# ============================== DB search ==============================
async def db_search_messages(conn, q2: str, chat: Optional[str], only_public: bool,
                             since: datetime, toks: list[str],
                             only_promo: bool, no_spam: bool,
                             limit: int, offset: int, max_per_channel: int,
                             user_key: Optional[str] = None,
                             intent=None):
    intent = intent or dict(include=[], exclude=[], strict=False, boost=0.0, penalty=0.0, sim_threshold=0.26)
    inc = intent.get("include", [])
    exc = intent.get("exclude", [])
    strict = bool(intent.get("strict", False))
    boost = float(intent.get("boost", 0.0))
    penalty = float(intent.get("penalty", 0.0))
    sim_thr = float(intent.get("sim_threshold", 0.26))

    rows = await conn.fetch(r"""
        WITH m2 AS (
         SELECT m.chat_id, m.message_id, m.date, m.text,
                m.views, m.forwards,
                c.username, c.title,
                lower(regexp_replace(coalesce(m.text,''), '\s+', ' ', 'g')) AS norm,
                (cardinality($6::text[]) = 0 OR EXISTS (
                    SELECT 1 FROM unnest($6::text[]) AS t WHERE lower(coalesce(m.text,'')) ILIKE ('%'||lower(t)||'%')
                )) AS kw_incl,
                (cardinality($7::text[]) > 0 AND EXISTS (
                    SELECT 1 FROM unnest($7::text[]) AS t WHERE lower(coalesce(m.text,'')) ILIKE ('%'||lower(t)||'%')
                )) AS kw_excl
         FROM messages m
         JOIN channels c USING(chat_id)
         WHERE m.date >= $2
           AND ($3::text IS NULL OR lower(c.username)=lower($3))
           AND ($4::bool IS FALSE OR c.username IS NOT NULL)
           AND length(coalesce(m.text,'')) >= 5
           AND (
               to_tsvector('russian', coalesce(m.text,'')) @@ (plainto_tsquery('russian', $1)
                   || websearch_to_tsquery('russian', $1))
            OR to_tsvector('english', coalesce(m.text,'')) @@ (plainto_tsquery('english', $1)
                   || websearch_to_tsquery('english', $1))
            OR similarity(lower(coalesce(m.text,'')), lower($1)) >= $11
            OR EXISTS (
                 SELECT 1 FROM unnest($5::text[]) AS t
                 WHERE lower(coalesce(m.text,'')) ILIKE ('%'||t||'%')
            )
           )
           AND (NOT $8::bool OR (
                 (cardinality($6::text[]) = 0 OR EXISTS (
                     SELECT 1 FROM unnest($6::text[]) AS t WHERE lower(coalesce(m.text,'')) ILIKE ('%'||lower(t)||'%')
                 ))
                 AND NOT (cardinality($7::text[]) > 0 AND EXISTS (
                     SELECT 1 FROM unnest($7::text[]) AS t WHERE lower(coalesce(m.text,'')) ILIKE ('%'||lower(t)||'%')
                 ))
           ))
        ),
        scored AS (
         SELECT m2.*,
                ts_rank_cd(to_tsvector('simple', m2.norm), plainto_tsquery('simple', $1)) * 0.55
              + similarity(lower(coalesce(m2.text,'')), lower($1)) * 0.45
              + (CASE WHEN m2.kw_incl THEN $9::float ELSE 0 END)
              - (CASE WHEN m2.kw_excl THEN $10::float ELSE 0 END) AS ft_score,
                md5(m2.norm) AS norm_hash
         FROM m2
        )
        SELECT DISTINCT ON (chat_id, norm_hash)
               chat_id, username, title, message_id, date, text, views, forwards, ft_score
        FROM scored
        ORDER BY chat_id, norm_hash, date DESC, views DESC
        LIMIT 2000
        """, q2, since, chat, only_public, toks, inc, exc, strict, boost, penalty, sim_thr)

    now = datetime.now(timezone.utc)
    items = []
    for r in rows:
        txt = r["text"] or ""
        if no_spam and is_spammy(txt): continue
        if only_promo and not is_promotional(txt): continue
        if intent.get("name") != "casino_promo" and GAMBLING_RE.search(txt or ""):
            continue
        w = recency_weight(r["date"], now)
        score = (float(r["ft_score"] or 0.0)*0.50 + w*0.30 + log1p(r["views"])*0.12 + log1p(r["forwards"])*0.08)
        ch_url, msg_url = make_links(r["username"], r["message_id"])
        items.append({
            "chat": r["username"] or r["title"],
            "chat_username": r["username"],
            "channel_url": ch_url,
            "message_url": msg_url,
            "chat_id": r["chat_id"],
            "message_id": r["message_id"],
            "date": r["date"].isoformat(),
            "snippet": (txt[:280] + "…") if len(txt) > 280 else txt,
            "score": score
        })

    random.shuffle(items)
    items.sort(key=lambda x: x["score"], reverse=True)
    items = _diversify(items, max_per_channel=max_per_channel)
    _, items = await _filter_and_record_antirepeat(conn, user_key, q2, items)

    total = len(items)
    items = items[offset:offset+limit]
    return {"total": total, "items": items}

async def db_search_chats(conn, q2: str, only_public: bool, since: datetime, toks: list[str],
                          only_promo: bool, no_spam: bool, limit: int,
                          user_key: Optional[str] = None,
                          intent=None):
    intent = intent or dict(include=[], exclude=[], strict=False, boost=0.0, penalty=0.0, sim_threshold=0.26)
    inc = intent.get("include", [])
    exc = intent.get("exclude", [])
    strict = bool(intent.get("strict", False))
    boost = float(intent.get("boost", 0.0))
    penalty = float(intent.get("penalty", 0.0))

    rows_msg = await conn.fetch(r"""
    WITH cand AS (
      SELECT m.chat_id, m.date, m.text, m.views, m.forwards,
             (cardinality($3::text[]) = 0 OR EXISTS (
                SELECT 1 FROM unnest($3::text[]) AS t WHERE lower(coalesce(m.text,'')) ILIKE ('%'||lower(t)||'%')
             )) AS kw_incl,
             (cardinality($4::text[]) > 0 AND EXISTS (
                SELECT 1 FROM unnest($4::text[]) AS t WHERE lower(coalesce(m.text,'')) ILIKE ('%'||lower(t)||'%')
             )) AS kw_excl
      FROM messages m
      WHERE m.date >= $2
        AND (
          to_tsvector('russian', coalesce(m.text,'')) @@ (plainto_tsquery('russian', $1)
             || websearch_to_tsquery('russian', $1))
          OR
          to_tsvector('english', coalesce(m.text,'')) @@ (plainto_tsquery('english', $1)
             || websearch_to_tsquery('english', $1))
          OR
          similarity(lower(coalesce(m.text,'')), lower($1)) >= 0.28
          OR
          EXISTS (
            SELECT 1 FROM unnest($5::text[]) AS t
            WHERE lower(coalesce(m.text,'')) ILIKE ('%'||t||'%')
          )
        )
    ),
    agg AS (
      SELECT c.chat_id, c.username, c.title,
             count(*) AS hits,
             max(cand.date) AS last_post,
             sum(cand.views) AS views_sum,
             sum(cand.forwards) AS fw_sum,
             sum(CASE WHEN cand.kw_incl THEN 1 ELSE 0 END) AS incl_hits,
             sum(CASE WHEN cand.kw_excl THEN 1 ELSE 0 END) AS excl_hits
      FROM cand
      JOIN channels c ON c.chat_id=cand.chat_id
      GROUP BY c.chat_id, c.username, c.title
    )
    SELECT * FROM agg
    WHERE ($6::bool IS FALSE OR username IS NOT NULL)
      AND (NOT $7::bool OR (incl_hits > 0 AND excl_hits = 0))
    ORDER BY last_post DESC
    LIMIT 1000
    """, q2, since, inc, exc, toks, only_public, strict)

    rows_meta = await conn.fetch(r"""
    SELECT chat_id, username, title, NOW()::timestamptz AS last_post,
           0::bigint AS hits, 0::bigint AS views_sum, 0::bigint AS fw_sum,
           0::bigint AS incl_hits, 0::bigint AS excl_hits
    FROM channels
    WHERE
      (to_tsvector('simple', coalesce(username,'') || ' ' || coalesce(title,'')) @@
       (plainto_tsquery('simple', $1) || websearch_to_tsquery('simple', $1)))
      OR lower(coalesce(username,'')) ILIKE '%'||lower($1)||'%'
      OR lower(coalesce(title,'')) ILIKE '%'||lower($1)||'%'
    LIMIT 600
    """, q2)

    now = datetime.now(timezone.utc)
    out_map: dict[int, dict] = {}

    def put_row(row, meta_boost=False):
        chat_id = int(row["chat_id"])
        username, title = row["username"], row["title"]
        ch_url, _ = make_links(username, None)
        score = (float(row["hits"])*0.45 +
                 recency_weight(row["last_post"], now)*0.35 +
                 log1p(row["views_sum"])*0.12 +
                 log1p(row["fw_sum"])*0.08 +
                 (boost if (row.get("incl_hits",0) and not row.get("excl_hits",0)) else 0.0) -
                 (penalty if (row.get("excl_hits",0) > 0) else 0.0) +
                 (0.3 if meta_boost else 0.0))
        out_map[chat_id] = {
            "chat_id": chat_id,
            "chat": username or title,
            "chat_username": username,
            "channel_url": ch_url,
            "title": title,
            "last_post": row["last_post"].isoformat() if row["last_post"] else None,
            "hits": int(row["hits"]),
            "score": float(score)
        }

    for r in rows_msg:
        if intent.get("name") != "casino_promo" and GAMBLING_RE.search(r["text"] or ""):
            continue
        put_row(r, meta_boost=False)

    for r in rows_meta:
        if int(r["chat_id"]) not in out_map:
            put_row(r, meta_boost=True)

    out = list(out_map.values())

    if only_promo or no_spam or intent.get("name") != "casino_promo":
        filtered = []
        for ch in out:
            if not ch["chat_username"]:
                continue
            ch_id = ch["chat_id"]
            msgs = await conn.fetch("SELECT text FROM messages WHERE chat_id=$1 ORDER BY date DESC LIMIT 30", ch_id)
            texts = [m["text"] or "" for m in msgs]
            if only_promo and not any(is_promotional(t) for t in texts): continue
            if no_spam and any(is_spammy(t) for t in texts): continue
            if intent.get("name") != "casino_promo" and any(GAMBLING_RE.search(t) for t in texts): continue
            filtered.append(ch)
        out = filtered

    random.shuffle(out)
    out.sort(key=lambda x: x["score"], reverse=True)
    return {"total": len(out), "items": out[:limit]}

# ============================== API ==============================
@app.get("/status")
async def status():
    pool = await db()
    async with pool.acquire() as conn:
        ch_cnt  = await conn.fetchval("SELECT count(*) FROM channels")
        msg_cnt = await conn.fetchval("SELECT count(*) FROM messages")
        last_dt = await conn.fetchval("SELECT max(date) FROM messages")
        qcnt    = await conn.fetchval("SELECT count(*) FROM crawl_queue")
        try:
            ext_cnt = await conn.fetchval("SELECT count(*) FROM external_usernames")
        except Exception:
            ext_cnt = None
    return {
        "channels": int(ch_cnt or 0),
        "messages": int(msg_cnt or 0),
        "queue": int(qcnt or 0),
        "external_usernames": (int(ext_cnt) if ext_cnt is not None else None),
        "last_message_at": last_dt.isoformat() if last_dt else None
    }

@app.get("/push_channels")
async def push_channels(usernames: str = Query(..., description="comma-separated usernames"),
                        days: int = 180, per: int = 300):
    us = [u.strip().lstrip("@") for u in usernames.split(",") if u.strip()]
    if not us:
        return {"ok": False, "error": "no usernames"}
    pool = await db()
    async with pool.acquire() as conn:
        async with (await SESS_POOL.client()) as cli:
            chans: list[Channel] = []
            for uname in us:
                try:
                    ent = await cli.get_entity(uname)
                    if isinstance(ent, Channel):
                        chans.append(ent)
                except RPCError:
                    continue
            if not chans:
                await push_usernames_to_queue(conn, us)
                return {"ok": True, "added": 0, "queued": len(us)}
            ch_new, msg_new = await live_backfill_messages(cli, conn, chans, days=days, per_channel=per)
            for ch in chans:
                await upsert_channel(conn, ch.id, getattr(ch,"username",None), getattr(ch,"title",None),
                                     "channel" if getattr(ch,"broadcast",False) else "supergroup")
    return {"ok": True, "added": len(chans)}

@app.get("/queue/add")
async def queue_add(usernames: str = Query(..., description="comma-separated @usernames")):
    us = [u.strip().lstrip("@") for u in usernames.split(",") if u.strip()]
    pool = await db()
    async with pool.acquire() as conn:
        await push_usernames_to_queue(conn, us)
    return {"ok": True, "queued": len(us)}

@app.post("/queue/seed")
async def queue_seed(body: dict):
    raw = body.get("usernames") or []
    if not isinstance(raw, list):
        return {"ok": False, "error": "field 'usernames' must be a list"}
    cleaned: List[str] = []
    for x in raw:
        if not x: continue
        s = str(x).strip()
        s = re.sub(r"^https?://t\.me/","", s, flags=re.I)
        s = s.lstrip("@").strip()
        if _is_valid_uname(s): cleaned.append(s.lower())
    cleaned = list(dict.fromkeys(cleaned))
    if not cleaned: return {"ok": False, "queued": 0}
    pool = await db()
    async with pool.acquire() as conn:
        await push_usernames_to_queue(conn, cleaned)
    return {"ok": True, "queued": len(cleaned)}

@app.get("/search_messages")
async def search_messages(
    q: str = Query(..., min_length=2),
    chat: Optional[str] = None,
    days: int = 90,
    only_promo: bool = True,
    only_public: bool = True,
    no_spam: bool = True,
    limit: int = 20,
    offset: int = 0,
    max_per_channel: int = 2,
    live: bool = False,
    u: Optional[str] = Query(None, description="user key for anti-repeats"),
):
    q2 = expand_query(q)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    toks = tokens_for_like(q2)
    intent = detect_intent(q)

    pool = await db()
    async with pool.acquire() as conn:
        if live and not _flood_blocked():
            try:
                async with (await SESS_POOL.client()) as cli:
                    chans = await live_find_channels(cli, q, limit=min(LIVE_MAX_CHATS,120))
                    if not chans:
                        widened = list(dict.fromkeys([q] + [q2] + CASINO_BRANDS + AFF_NETWORKS))
                        random.shuffle(widened)
                        for wq in widened[:5]:
                            cs = await live_find_channels(cli, wq, limit=40)
                            chans += cs
                            if len(chans) >= LIVE_MAX_CHATS: break
                    if chans:
                        chans = chans[:LIVE_MAX_CHATS]
                        await asyncio.sleep(0.2 + random.random()*0.4)
                        await live_backfill_messages(cli, conn, chans, days=days, per_channel=min(LIVE_PER_CHANNEL_LIMIT,160))
            except FloodWaitError as e:
                _set_flood_block(getattr(e,"seconds",30))
            except Exception as e:
                logging.warning(f"Live phase failed: {e}")

        return JSONResponse(await db_search_messages(
            conn, q2, chat, only_public, since, toks, only_promo, no_spam,
            limit, offset, max_per_channel, user_key=u, intent=intent
        ))

@app.get("/search_chats")
async def search_chats(
    q: str = Query(..., min_length=2),
    days: int = 90,
    only_promo: bool = True,
    only_public: bool = True,
    no_spam: bool = True,
    limit: int = 15,
    live: bool = False,
    u: Optional[str] = Query(None, description="user key (optional)"),
):
    q2 = expand_query(q)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    toks = tokens_for_like(q2)
    intent = detect_intent(q)

    pool = await db()
    async with pool.acquire() as conn:
        if live and not _flood_blocked():
            try:
                async with (await SESS_POOL.client()) as cli:
                    chans = await live_find_channels(cli, q, limit=min(LIVE_MAX_CHATS,120))
                    if chans:
                        chans = chans[:LIVE_MAX_CHATS]
                        await asyncio.sleep(0.2 + random.random()*0.4)
                        await live_backfill_messages(cli, conn, chans, days=days, per_channel=min(LIVE_KNOWN_PER_CHANNEL,160))
            except FloodWaitError as e:
                _set_flood_block(getattr(e,"seconds",30))
            except Exception as e:
                logging.warning(f"Live channels failed: {e}")

        res = await db_search_chats(conn, q2, only_public, since, toks, only_promo, no_spam, limit, user_key=u, intent=intent)
        return JSONResponse(res)

# ============================== ENTRYPOINT ==============================
async def check_session():
    async with (await SESS_POOL.client()) as cli:
        me = await cli.get_me()
        logging.info(f"MTProto OK. Logged in as: {me.first_name} (bot={getattr(me,'bot',False)})")

async def main():
    import uvicorn
    loop = asyncio.get_running_loop()
    await check_session()

    if START_USERNAMES:
        pool = await db()
        async with pool.acquire() as conn:
            await push_usernames_to_queue(conn, [u.lstrip("@") for u in START_USERNAMES])

    if RUN_CRAWLER:
        logging.info("Starting crawler loop…")
        loop.create_task(crawler_loop())

    # AUTO_SEED_ENABLED оставлен как конфиг-переменная; авто-сидер здесь не запускаем

    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.getenv("PORT","8000")))
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
