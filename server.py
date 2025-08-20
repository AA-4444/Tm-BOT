import os, asyncio, re, math, logging, random, time, hashlib, json
from datetime import timezone, datetime, timedelta
from typing import Optional, Iterable, List, Tuple, Dict, Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import asyncpg

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError
from telethon.errors.rpcerrorlist import (
    UsernameNotOccupiedError, UsernameInvalidError, ChannelInvalidError
)
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel, Message, MessageService

logging.basicConfig(level=logging.INFO)

# ============================== ENV ==============================
API_ID  = int(os.getenv("API_ID","0"))
API_HASH = os.getenv("API_HASH","")

# one or many StringSession: TELEGRAM_STRING_SESSIONS="sess1,sess2,..." (spaces allowed)
SESS_LIST = [s.strip() for s in os.getenv("TELEGRAM_STRING_SESSIONS","").split(",") if s.strip()]
if not SESS_LIST and os.getenv("TELEGRAM_STRING_SESSION",""):
    SESS_LIST = [os.getenv("TELEGRAM_STRING_SESSION").strip()]

if not API_ID or not API_HASH:
    logging.warning("API_ID/API_HASH are not set; Telethon client will fail to login.")

PG_DSN = os.getenv("DATABASE_URL","postgresql://postgres:postgres@localhost:5432/postgres")

RUN_CRAWLER = os.getenv("RUN_CRAWLER","0").lower() in ("1","true","yes")
LIVE_MAX_CHATS = int(os.getenv("LIVE_MAX_CHATS", "120"))
LIVE_PER_CHANNEL_LIMIT = int(os.getenv("LIVE_PER_CHANNEL_LIMIT", "220"))
LIVE_KNOWN_PER_CHANNEL = int(os.getenv("LIVE_KNOWN_PER_CHANNEL", "200"))
CRAWL_QUEUE_BATCH = int(os.getenv("CRAWL_QUEUE_BATCH", "120"))
MAX_PARALLEL_CHANNELS = int(os.getenv("MAX_PARALLEL_CHANNELS", "6"))
MAX_ITEMS_PER_QUERY = int(os.getenv("MAX_ITEMS_PER_QUERY", "2000"))
DIVERSIFY_MAX_PER_CHANNEL = int(os.getenv("DIVERSIFY_MAX_PER_CHANNEL", "2"))

# optional admin token for protected routes (NULL = disabled)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN","")

app = FastAPI(title="TG Search API")
_db_pool: asyncpg.Pool | None = None

# global flood guard
_flood_block_until: float = 0.0  # unix ts

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

-- who/what we already served (anti-duplicates)
CREATE TABLE IF NOT EXISTS served_hits(
  user_key TEXT,
  qhash TEXT,
  chat_id BIGINT,
  message_id BIGINT,
  ts TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY(user_key, qhash, chat_id, message_id)
);

-- usernames to be crawled
CREATE TABLE IF NOT EXISTS crawl_queue(
  username TEXT PRIMARY KEY,
  last_try TIMESTAMPTZ,
  tries INT DEFAULT 0
);

-- optional channel metadata
ALTER TABLE channels
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;

-- indexes
CREATE INDEX IF NOT EXISTS ix_msg_tsv_ru ON messages
USING GIN (to_tsvector('russian', coalesce(text,'')));

CREATE INDEX IF NOT EXISTS ix_msg_tsv_en ON messages
USING GIN (to_tsvector('english', coalesce(text,'')));

CREATE INDEX IF NOT EXISTS ix_msg_date ON messages(date DESC);

CREATE INDEX IF NOT EXISTS ix_msg_trgm ON messages
USING GIN ((lower(coalesce(text,''))) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS ix_chan_name_ru ON channels
USING GIN (to_tsvector('russian', coalesce(title,'')));
"""

# ============================== DB pool ==============================
async def db():
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=10)
        async with _db_pool.acquire() as c:
            await c.execute(DDL)
    return _db_pool

# ============================== SEEDS ==============================
# Rich seeds for casino / gambling / affiliates
CASINO_BRANDS = [
    "1xbet","pin-up","pinnacle","joycasino","vulkan","parimatch","fonbet","leonbets",
    "bet365","ggbet","melbet","betfair","betway","winline","mostbet","888casino",
    "pokerstars","partypoker","unibet","22bet","mr green","stake","roobet","cloudbet",
    "playamo","bitstarz","golden star","parimatch casino","bwin","william hill",
    "betcity","olymp bet","betsafe","dafabet","10bet","betano","pokerok","ggpoker",
    "gg poker","betsson","ladbrokes","casumo","mr bet","national casino","hollywoodbets",
]

AFF_NETWORKS = [
    "cpa","cpa gambling","affiliate casino","affiliate marketing","revshare","hybrid deal",
    "apikings","alpha affiliates","leads.su","everad","adsterra","propellerads","leadbit",
    "clickdealer","maxbounty","awempire","traffic partners","gagarin partners","income access",
    "bet365 affiliates","parimatch affiliates","pinnacle affiliates","ggbet affiliates",
    "mediabuying","affiliate network","ofer","offerwall","smartlink","tracking platform",
    "hasoffers","voluum","keitaro","redtrack","binom tracker",
]

CASINO_RU = [
    "казино","онлайн казино","фриспины","бесплатные вращения","бездепозитный бонус","промокод казино",
    "бонус казино","слоты","игровые автоматы","рулетка","блэкджек","покер","букмекер",
    "ставки на спорт","беттинг","фрибет","кэшбек","выплаты","вывод средств",
]

CASINO_EN = [
    "casino","gambling","free spins","bonus code","no deposit bonus","slots","slot machines",
    "roulette","blackjack","poker","bookmaker","betting","freebet","cashback","withdrawal",
]

GENERAL_WIDE_SEEDS = [
    "marketing","seo","smm","арбитраж трафика","трафик","промокод","скидки","купоны",
    "партнерская программа","заработок в интернете","рынок рекламы","influencer","контент маркетинг",
    "диджитал","digital marketing","email marketing","push traffic","popunder","native ads",
]

# verified public channels to pre-seed crawl_queue (optional)
START_USERNAMES = [
    # examples (fill with your own to kickstart graph)
    # "parimatchru","winline","1xstavka","pinupcasino","alpha_affiliates"
]

SEEDS_ALL = list(dict.fromkeys(CASINO_BRANDS + AFF_NETWORKS + CASINO_RU + CASINO_EN + GENERAL_WIDE_SEEDS))

# ============================== Helpers ==============================
SYNONYMS = {
    r"\bфри[-\s]?спин(ы|ов|а)?\b": ["free spin", "free spins", "фриспин", "спины", "бесплатные вращения"],
    r"\bбездеп(озит(ный|а)?)?\b": ["без депозита", "no deposit", "бездепозитный", "ND bonus", "no-deposit"],
    r"\bбонус(ы|ов)?\b": ["bonus", "бонус код", "промокод", "promo code", "промо-код", "cashback", "кэшбек", "вейджер", "wager"],
    r"\bфриспины\b": ["free spins","спины","бесплатные вращения"],
    r"\bказино\b": ["casino", "онлайн казино", "slots", "слоты", "игровые автоматы"],
    r"\baffiliate\b": ["aff", "арбитраж", "партнерка", "партнёрка", "revshare", "hybrid", "cpa", "cpa gambling", "affiliate marketing"],
    r"\bарбитраж\b": ["traffic", "арбитраж трафика", "affiliate", "aff", "cpa"],
    r"\bпромокод\b": ["bonus code", "promo", "coupon", "код"],
}

PROMO_POS = re.compile(r"(бонус|free\s*spins?|фри[-\s]?спин|промо-?код|промокод|бездеп(озит)|депозит\s*бонус|welcome|фриспин|cash\s*back|кэшбек|промо)", re.I)
PROMO_NEG = re.compile(r"(мем|шутк|юмор|сарказм|ирони|мемы|прикол)", re.I)
SPAM_NEG = re.compile(
    r"(free\s*movies?|tv\s*shows?|stream|прям(ая|ые)\s*трансляц|расписани[ея]|schedule|fixtures|"
    r"live\s*score|match|vs\s|лига|серия\s*a|серия\s*b|кубак?|epl|la\s*liga|bundesliga|"
    r"прогноз(ы)?\s*на\s*матч|ставк(и|а)\s*на\s*спорт|коэфф(ициент)?|odds|parlay)",
    re.I
)

TG_USERNAME_RE = re.compile(r"(?:^|[\s:;/\(\)\[\]\.,])@([a-zA-Z0-9_]{4,32})\b|https?://t\.me/([a-zA-Z0-9_]{4,32})\b")

def expand_query(q: str) -> str:
    ql = q.lower()
    extra = []
    for pat, syns in SYNONYMS.items():
        if re.search(pat, ql):
            extra += syns
    if extra:
        q = q + " " + " ".join(sorted(set(extra)))
    return q

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
    try:
        return math.log1p(max(0.0, float(x)))
    except Exception:
        return 0.0

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
AFFILIATE_INCLUDE = [
    "affiliate","affiliates","aff","арбитраж","арбитраж трафика","партнерка","партнёрка",
    "ревшар","revshare","hybrid","cpa","lead","offer","оффер","media buying","buying",
    "тимлид","team lead","тим лид","сетка офферов","smartlink","tracker","tracking",
    "voluum","keitaro","redtrack","binom","spy tool","nutra","gambling","betting","игорка",
]
AFFILIATE_EXCLUDE = [
    "free spins","фриспин","фриспины","промокод","бездеп","депозитный бонус","welcome bonus",
    "ставки на спорт","фрибет","розыгрыш","конкурс"
]

JOBS_INCLUDE = ["vacancy","ваканси","вакансии","job","работа","ищем","позиция","position","hiring","junior","middle","senior"]
JOBS_EXCLUDE = []

CASINO_PROMO_INCLUDE = ["free spins","фриспины","бонус","промокод","no deposit","бездеп","cashback","кэшбек","welcome"]
CASINO_PROMO_EXCLUDE = ["ваканс","job","affiliate","арбитраж","партнерк"]

def detect_intent(q: str):
    ql = q.lower()
    def anyw(words): return any(w in ql for w in words)
    if anyw(["affiliate","арбитраж","партнерк","cpa","revshare","hybrid"]):
        return dict(name="affiliate", include=AFFILIATE_INCLUDE, exclude=AFFILIATE_EXCLUDE, strict=True, boost=0.35, penalty=0.25)
    if anyw(["ваканс","vacancy","job","работа","hiring"]):
        return dict(name="jobs", include=JOBS_INCLUDE, exclude=JOBS_EXCLUDE, strict=False, boost=0.25, penalty=0.0)
    if anyw(["casino","казино","фриспин","free spins","бонус","промокод"]):
        return dict(name="casino_promo", include=CASINO_PROMO_INCLUDE, exclude=CASINO_PROMO_EXCLUDE, strict=False, boost=0.25, penalty=0.15)
    return dict(name="generic", include=[], exclude=[], strict=False, boost=0.0, penalty=0.0)

# ============================== Live helpers (Telegram) ==============================
def make_links(username: Optional[str], message_id: Optional[int]):
    if not username:
        return None, None
    ch = f"https://t.me/{username}"
    msg = f"{ch}/{message_id}" if message_id else None
    return ch, msg

def channels_only(chats: Iterable) -> list[Channel]:
    return [c for c in chats if isinstance(c, Channel) and getattr(c,"username",None)]

def _flood_blocked() -> bool:
    return time.time() < _flood_block_until

def _set_flood_block(seconds: int):
    global _flood_block_until
    _flood_block_until = time.time() + max(5, seconds)

# ============================== Session Pool ==============================
class SessionPool:
    def __init__(self, sessions: List[str]):
        self.sessions = [s for s in sessions if s]
        self.idx = 0
        if not self.sessions:
            raise RuntimeError("No TELEGRAM sessions provided")
    def next_session(self) -> StringSession:
        s = self.sessions[self.idx % len(self.sessions)]
        self.idx += 1
        return StringSession(s)
    async def client(self) -> TelegramClient:
        return TelegramClient(self.next_session(), API_ID, API_HASH)

SESS_POOL = SessionPool(SESS_LIST)

# ============================== Upserts ==============================
async def upsert_channel(conn, chat_id, username, title, typ):
    await conn.execute("""
    INSERT INTO channels(chat_id, username, title, type)
    VALUES($1,$2,$3,$4)
    ON CONFLICT (chat_id) DO UPDATE
      SET username=EXCLUDED.username, title=EXCLUDED.title, type=EXCLUDED.type
    """, chat_id, username, title, typ)

async def upsert_message(conn, row):
    await conn.execute("""
    INSERT INTO messages(chat_id, message_id, date, text, has_media, links, views, forwards)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (chat_id, message_id) DO NOTHING
    """, row["chat_id"], row["message_id"], row["date"], row["text"], row["has_media"],
         row["links"], row["views"], row["forwards"])

# ============================== Live operations ==============================
async def live_find_channels(cli: TelegramClient, query: str, limit: int = 80) -> list[Channel]:
    if _flood_blocked():
        return []
    try:
        res = await cli(SearchRequest(q=query, limit=limit))
    except FloodWaitError as e:
        logging.warning(f"FloodWait {e.seconds}s on SearchRequest")
        _set_flood_block(e.seconds)
        return []
    except RPCError:
        return []
    return channels_only(res.chats)

async def live_backfill_messages(cli: TelegramClient, conn, channels: list[Channel], days: int, per_channel: int = 250):
    since_dt = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    sem = asyncio.Semaphore(MAX_PARALLEL_CHANNELS)

    async def _one(ch: Channel):
        async with sem:
            try:
                ent = await cli.get_entity(ch)
            except RPCError:
                return
            await upsert_channel(
                conn,
                ent.id,
                getattr(ent, "username", None),
                getattr(ent, "title", None),
                "channel" if getattr(ent, "broadcast", False) else "supergroup",
            )
            async for msg in cli.iter_messages(ent, limit=per_channel):
                dt = (msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)) if msg.date else None
                if dt and dt < since_dt:
                    break
                text = (getattr(msg, "message", None) or "") if not isinstance(msg, MessageService) else ""
                row = {
                    "chat_id": ent.id,
                    "message_id": msg.id,
                    "date": dt or datetime.now(timezone.utc),
                    "text": text,
                    "has_media": bool(getattr(msg, "media", None)),
                    "links": len(re.findall(r"https?://", text)),
                    "views": getattr(msg, "views", 0) or 0,
                    "forwards": getattr(msg, "forwards", 0) or 0,
                }
                await upsert_message(conn, row)

                # created_at backfill (first seen)
                await conn.execute("""
                    UPDATE channels c SET created_at = COALESCE(c.created_at, $2)
                    WHERE c.chat_id = $1
                """, ent.id, dt or datetime.now(timezone.utc))

                if text:
                    us = extract_usernames_from_text(text)
                    if us:
                        await push_usernames_to_queue(conn, us)

    tasks = [_one(ch) for ch in channels]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# ============================== Crawl / Queue ==============================
async def crawl_from_queue(per_channel: int = 300, batch: int = CRAWL_QUEUE_BATCH):
    if _flood_blocked():
        return
    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT username FROM crawl_queue
            ORDER BY COALESCE(last_try, to_timestamp(0)) ASC
            LIMIT $1
        """, batch)
        if not rows:
            return
        usernames = [r["username"] for r in rows]
        await conn.executemany(
            "UPDATE crawl_queue SET last_try=NOW(), tries=COALESCE(tries,0)+1 WHERE username=$1",
            [(u,) for u in usernames]
        )

        to_delete: list[tuple[str]] = []
        chans: List[Channel] = []

        try:
            async with (await SESS_POOL.client()) as cli:
                for uname in usernames:
                    try:
                        ent = await cli.get_entity(uname)
                        if isinstance(ent, Channel) and getattr(ent,"username",None):
                            chans.append(ent)
                        else:
                            to_delete.append((uname,))
                    except (UsernameNotOccupiedError, UsernameInvalidError, ChannelInvalidError, ValueError):
                        to_delete.append((uname,))
                    except FloodWaitError as e:
                        _set_flood_block(e.seconds)
                        break
                    except RPCError:
                        continue
                if chans:
                    await live_backfill_messages(cli, conn, chans, days=365, per_channel=per_channel)
        finally:
            if to_delete:
                await conn.executemany("DELETE FROM crawl_queue WHERE username=$1", to_delete)

async def crawl_once(seeds: list[str], per_channel: int = 500):
    if _flood_blocked():
        return
    pool = await db()
    async with pool.acquire() as conn:
        try:
            async with (await SESS_POOL.client()) as cli:
                seeded = list(dict.fromkeys(seeds + SEEDS_ALL))
                random.shuffle(seeded)
                for q in seeded:
                    chans = await live_find_channels(cli, q, limit=min(LIVE_MAX_CHATS, 120))
                    if not chans:
                        continue
                    await live_backfill_messages(cli, conn, chans, days=365, per_channel=per_channel)
        except FloodWaitError as e:
            _set_flood_block(e.seconds)

async def crawler_loop():
    seeds = list(dict.fromkeys(SEEDS_ALL))
    while True:
        try:
            logging.info("Crawler: cycle start")
            await crawl_from_queue(per_channel=350, batch=CRAWL_QUEUE_BATCH)
            await crawl_once(seeds, per_channel=320)
            logging.info("Crawler: cycle end")
        except Exception as e:
            logging.error(f"Crawler error: {e}")
        await asyncio.sleep(60*20)

# ============================== Common search helpers ==============================
def _diversify(items: list[dict], max_per_channel: int = DIVERSIFY_MAX_PER_CHANNEL) -> list[dict]:
    seen = {}
    out = []
    for it in items:
        ch = it.get("chat_username") or it.get("chat")
        c = seen.get(ch, 0)
        if c < max_per_channel:
            out.append(it)
            seen[ch] = c + 1
    return out

async def _apply_antirepeat_and_record(conn, user_key: Optional[str], q: str, items: List[dict]) -> Tuple[int,List[dict]]:
    if not user_key:
        return len(items), items
    h = qhash(q)
    filt = []
    for it in items:
        cid = it.get("chat_id") or None
        mid = it.get("message_id") or None
        if cid and mid:
            seen = await conn.fetchval("""
                SELECT 1 FROM served_hits WHERE user_key=$1 AND qhash=$2 AND chat_id=$3 AND message_id=$4
            """, user_key, h, cid, mid)
            if seen:
                continue
        filt.append(it)
    rows = [(user_key, h, it.get("chat_id"), it.get("message_id")) for it in filt if it.get("chat_id") and it.get("message_id")]
    if rows:
        await conn.executemany("""
            INSERT INTO served_hits(user_key, qhash, chat_id, message_id)
            VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING
        """, rows)
    return len(items), filt

# ============================== DB search (intent-aware) ==============================
async def db_search_messages(conn, q2: str, chat: Optional[str], only_public: bool,
                             since: datetime, toks: list[str],
                             only_promo: bool, no_spam: bool,
                             limit: int, offset: int, max_per_channel: int,
                             user_key: Optional[str] = None,
                             intent=None):
    intent = intent or dict(include=[], exclude=[], strict=False, boost=0.0, penalty=0.0)
    inc = intent.get("include", [])
    exc = intent.get("exclude", [])
    strict = bool(intent.get("strict", False))
    boost = float(intent.get("boost", 0.0))
    penalty = float(intent.get("penalty", 0.0))

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
        AND (
          to_tsvector('russian', coalesce(m.text,'')) @@ (plainto_tsquery('russian', $1)
             || websearch_to_tsquery('russian', $1))
          OR
          to_tsvector('english', coalesce(m.text,'')) @@ (plainto_tsquery('english', $1)
             || websearch_to_tsquery('english', $1))
          OR
          similarity(lower(coalesce(m.text,'')), lower($1)) >= 0.20
          OR
          EXISTS (
            SELECT 1 FROM unnest($5::text[]) AS t
            WHERE lower(coalesce(m.text,'')) ILIKE ('%'||t||'%')
          )
        )
        AND (NOT $8::bool OR ( (cardinality($6::text[]) = 0 OR EXISTS (
                SELECT 1 FROM unnest($6::text[]) AS t WHERE lower(coalesce(m.text,'')) ILIKE ('%'||lower(t)||'%')
            )) AND NOT (cardinality($7::text[]) > 0 AND EXISTS (
                SELECT 1 FROM unnest($7::text[]) AS t WHERE lower(coalesce(m.text,'')) ILIKE ('%'||lower(t)||'%')
            )) ))
    ),
    scored AS (
      SELECT m2.*,
             ts_rank_cd(to_tsvector('simple', m2.norm), plainto_tsquery('simple', $1)) * 0.55
               + similarity(lower(coalesce(m2.text,'')), lower($1)) * 0.45
               + (CASE WHEN m2.kw_incl THEN $9::float ELSE 0 END)
               - (CASE WHEN m2.kw_excl THEN $10::float ELSE 0 END)
               AS ft_score,
             md5(m2.norm) AS norm_hash
      FROM m2
    )
    SELECT DISTINCT ON (chat_id, norm_hash)
           chat_id, username, title, message_id, date, text, views, forwards, ft_score
    FROM scored
    ORDER BY chat_id, norm_hash, date DESC
    LIMIT $11
    """, q2, since, chat, only_public, toks, inc, exc, strict, boost, penalty, MAX_ITEMS_PER_QUERY)

    now = datetime.now(timezone.utc)
    items = []
    for r in rows:
        txt = r["text"] or ""
        if no_spam and is_spammy(txt):
            continue
        if only_promo and not is_promotional(txt):
            continue
        w = recency_weight(r["date"], now)
        score = (
            float(r["ft_score"] or 0.0) * 0.50 +
            w * 0.30 +
            log1p(r["views"]) * 0.12 +
            log1p(r["forwards"]) * 0.08
        )
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

    items.sort(key=lambda x: x["score"], reverse=True)
    items = _diversify(items, max_per_channel=max_per_channel)
    _, items = await _apply_antirepeat_and_record(conn, user_key, q2, items)
    total = len(items)
    items = items[offset:offset+limit]
    return {"total": total, "items": items}

async def db_search_chats(conn, q2: str, only_public: bool, since: datetime, toks: list[str],
                          only_promo: bool, no_spam: bool, limit: int,
                          user_key: Optional[str] = None,
                          intent=None):
    intent = intent or dict(include=[], exclude=[], strict=False, boost=0.0, penalty=0.0)
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
          similarity(lower(coalesce(m.text,'')), lower($1)) >= 0.20
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
    SELECT *
    FROM agg
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
    out_map: Dict[int, Dict[str, Any]] = {}

    def put_row(row, meta_boost=False):
        chat_id = int(row["chat_id"])
        username, title = row["username"], row["title"]
        ch_url, _ = make_links(username, None)
        score = (
            float(row["hits"]) * 0.45 +
            recency_weight(row["last_post"], now) * 0.35 +
            log1p(row["views_sum"]) * 0.12 +
            log1p(row["fw_sum"]) * 0.08 +
            (boost if (row.get("incl_hits",0) and not row.get("excl_hits",0)) else 0.0) -
            (penalty if (row.get("excl_hits",0) > 0) else 0.0) +
            (0.3 if meta_boost else 0.0)
        )
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
        put_row(r, meta_boost=False)
    for r in rows_meta:
        if int(r["chat_id"]) not in out_map:
            put_row(r, meta_boost=True)

    out = list(out_map.values())

    # filters on latest 30 posts
    if only_promo or no_spam:
        filtered = []
        for ch in out:
            if not ch["chat_username"]:
                continue
            ch_id = ch["chat_id"]
            msgs = await conn.fetch("""
                SELECT text FROM messages WHERE chat_id=$1
                ORDER BY date DESC LIMIT 30
            """, ch_id)
            texts = [m["text"] or "" for m in msgs]
            if only_promo and not any(is_promotional(t) for t in texts):
                continue
            if no_spam and any(is_spammy(t) for t in texts):
                continue
            filtered.append(ch)
        out = filtered

    out.sort(key=lambda x: x["score"], reverse=True)
    return {"total": len(out), "items": out[:limit]}

# ============================== API ==============================
@app.get("/status")
async def status():
    pool = await db()
    async with pool.acquire() as conn:
        ch_cnt = await conn.fetchval("SELECT count(*) FROM channels")
        msg_cnt = await conn.fetchval("SELECT count(*) FROM messages")
        last_dt = await conn.fetchval("SELECT max(date) FROM messages")
        qcnt  = await conn.fetchval("SELECT count(*) FROM crawl_queue")
    return {
        "channels": ch_cnt,
        "messages": msg_cnt,
        "queue": qcnt,
        "last_message_at": last_dt.isoformat() if last_dt else None
    }

@app.get("/stats/growth")
async def stats_growth():
    pool = await db()
    async with pool.acquire() as conn:
        ch_total = await conn.fetchval("SELECT COUNT(*) FROM channels")
        ch_new7  = await conn.fetchval("SELECT COUNT(*) FROM channels WHERE created_at > now()-interval '7 days'")
        ch_24h   = await conn.fetchval("SELECT COUNT(DISTINCT chat_id) FROM messages WHERE date > now() - interval '24 hours'")
        msg_24h  = await conn.fetchval("SELECT COUNT(*) FROM messages WHERE date > now() - interval '24 hours'")
        qsize    = await conn.fetchval("SELECT COUNT(*) FROM crawl_queue")
    return {
        "channels_total": ch_total,
        "channels_new_7d": ch_new7,
        "channels_touched_24h": ch_24h,
        "messages_24h": msg_24h,
        "queue_size": qsize,
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
                    if isinstance(ent, Channel) and getattr(ent,"username",None):
                        chans.append(ent)
                except RPCError:
                    continue
            if not chans:
                await push_usernames_to_queue(conn, us)
                return {"ok": True, "added": 0, "queued": len(us)}
            await live_backfill_messages(cli, conn, chans, days=days, per_channel=per)
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
    max_per_channel: int = DIVERSIFY_MAX_PER_CHANNEL,
    live: bool = True,
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
                    chans = await live_find_channels(cli, q, limit=min(LIVE_MAX_CHATS, 120))
                    if not chans:
                        widened = list(dict.fromkeys([q] + [q2] + CASINO_BRANDS + AFF_NETWORKS))
                        random.shuffle(widened)
                        for wq in widened[:5]:
                            cs = await live_find_channels(cli, wq, limit=40)
                            chans += cs
                            if len(chans) >= LIVE_MAX_CHATS:
                                break
                    if chans:
                        await live_backfill_messages(cli, conn, chans[:LIVE_MAX_CHATS], days=days, per_channel=LIVE_PER_CHANNEL_LIMIT)
            except FloodWaitError as e:
                _set_flood_block(e.seconds)
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
    live: bool = True,
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
                    chans = await live_find_channels(cli, q, limit=min(LIVE_MAX_CHATS, 120))
                    if chans:
                        await live_backfill_messages(cli, conn, chans[:LIVE_MAX_CHATS], days=days, per_channel=LIVE_KNOWN_PER_CHANNEL)
            except FloodWaitError as e:
                _set_flood_block(e.seconds)
            except Exception as e:
                logging.warning(f"Live channels failed: {e}")

        res = await db_search_chats(conn, q2, only_public, since, toks, only_promo, no_spam, limit, user_key=u, intent=intent)
        return JSONResponse(res)

# --------- Admin/Debug (optional) ---------
@app.get("/debug/flood")
async def debug_flood():
    return {"flood_blocked": _flood_blocked(), "unblock_in_sec": max(0, int(_flood_block_until - time.time()))}

@app.get("/admin/reindex")
async def admin_reindex(token: Optional[str] = Query(None)):
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    pool = await db()
    async with pool.acquire() as conn:
        # cheap "touch" to ensure DDL exists; true reindex is heavy and not done here
        await conn.execute("SELECT 1")
    return {"ok": True}

# ============================== ENTRYPOINT ==============================
async def check_session():
    async with (await SESS_POOL.client()) as cli:
        me = await cli.get_me()
        logging.info(f"MTProto OK. Logged in as: {me.first_name} (bot={getattr(me,'bot',False)})")

async def main():
    import uvicorn
    loop = asyncio.get_running_loop()
    await check_session()

    # preseed queue
    if START_USERNAMES:
        pool = await db()
        async with pool.acquire() as conn:
            await push_usernames_to_queue(conn, [u.lstrip("@") for u in START_USERNAMES])

    if RUN_CRAWLER:
        logging.info("Starting crawler loop…")
        loop.create_task(crawler_loop())

    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.getenv("PORT","8000")))
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
