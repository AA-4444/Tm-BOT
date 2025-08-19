import os, asyncio, re, math, logging, random
from datetime import timezone, datetime, timedelta
from typing import Optional, Iterable

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import asyncpg

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel

logging.basicConfig(level=logging.INFO)

API_ID  = int(os.getenv("API_ID","0"))
API_HASH = os.getenv("API_HASH","")
STRING_SESSION = os.getenv("TELEGRAM_STRING_SESSION","")
PG_DSN = os.getenv("DATABASE_URL","postgresql://postgres:postgres@localhost:5432/postgres")

RUN_CRAWLER = os.getenv("RUN_CRAWLER","0") in ("1","true","yes")

app = FastAPI()
_db_pool: asyncpg.Pool | None = None

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

-- FTS (ru+en) и свежесть
CREATE INDEX IF NOT EXISTS ix_msg_tsv_ru ON messages
USING GIN (to_tsvector('russian', coalesce(text,'')));
CREATE INDEX IF NOT EXISTS ix_msg_tsv_en ON messages
USING GIN (to_tsvector('english', coalesce(text,'')));
CREATE INDEX IF NOT EXISTS ix_msg_date ON messages(date DESC);

-- ВАЖНО: без unaccent в индексе (unaccent не IMMUTABLE)
CREATE INDEX IF NOT EXISTS ix_msg_trgm ON messages
USING GIN ( (lower(coalesce(text,''))) gin_trgm_ops );

CREATE INDEX IF NOT EXISTS ix_chan_name_ru ON channels
USING GIN (to_tsvector('russian', coalesce(title,'')));
"""

# ---------------- DB ----------------
async def db():
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=8)
        async with _db_pool.acquire() as c:
            await c.execute(DDL)
    return _db_pool

# ---------------- Search helpers ----------------
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
BRAND_SEEDS = [
    "1xbet", "pin-up", "pinnacle", "joycasino", "vulkan", "parimatch", "fonbet",
    "leonbets", "bet365", "ggbet", "melbet", "betfair", "betway", "winline",
]

def expand_query(q: str) -> str:
    ql = q.lower()
    extra = []
    for pat, syns in SYNONYMS.items():
        if re.search(pat, ql):
            extra += syns
    if extra:
        q = q + " " + " ".join(sorted(set(extra)))
    return q

PROMO_POS = re.compile(r"(бонус|free\s*spins?|фри[-\s]?спин|промо-?код|промокод|бездеп(озит)|депозит\s*бонус|welcome|фриспин|cash\s*back|кэшбек|промо)", re.I)
PROMO_NEG = re.compile(r"(мем|шутк|юмор|сарказм|ирони|мемы|прикол)", re.I)

SPAM_NEG = re.compile(
    r"(free\s*movies?|tv\s*shows?|stream|прям(ая|ые)\s*трансляц|расписани[ея]|schedule|fixtures|"
    r"live\s*score|match|vs\s|лига|серия\s*a|серия\s*b|кубак?|epl|la\s*liga|bundesliga|"
    r"прогноз(ы)?\s*на\s*матч|ставк(и|а)\s*на\s*спорт|коэфф(ициент)?|odds|parlay)",
    re.I
)

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
    return [t for t in toks if len(t) >= 3][:8]

# ---------------- Telegram helpers (live) ----------------
def make_links(username: Optional[str], message_id: Optional[int]):
    if not username:
        return None, None
    ch = f"https://t.me/{username}"
    msg = f"{ch}/{message_id}" if message_id else None
    return ch, msg

def channels_only(chats: Iterable) -> list[Channel]:
    out = []
    for c in chats:
        if isinstance(c, Channel) and getattr(c, "username", None):
            out.append(c)
    return out

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

async def live_find_channels(cli: TelegramClient, query: str, limit: int = 80) -> list[Channel]:
    try:
        res = await cli(SearchRequest(q=query, limit=limit))
    except FloodWaitError as e:
        logging.warning(f"FloodWait {e.seconds}s on SearchRequest")
        await asyncio.sleep(e.seconds + 2)
        res = await cli(SearchRequest(q=query, limit=min(20, limit)))
    except RPCError:
        return []
    return channels_only(res.chats)

async def live_backfill_messages(cli: TelegramClient, conn, channels: list[Channel], days: int, per_channel: int = 250):
    since_dt = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    sem = asyncio.Semaphore(6)

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
                if msg.date and (msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)) < since_dt:
                    break
                text = msg.message or ""
                row = {
                    "chat_id": ent.id,
                    "message_id": msg.id,
                    "date": (msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)),
                    "text": text,
                    "has_media": bool(msg.media),
                    "links": len(re.findall(r"https?://", text)),
                    "views": getattr(msg, "views", 0) or 0,
                    "forwards": getattr(msg, "forwards", 0) or 0,
                }
                await upsert_message(conn, row)

    tasks = [_one(ch) for ch in channels]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# ---------------- Crawler (optional) ----------------
async def crawl_once(seeds: list[str], per_channel: int = 500):
    pool = await db()
    async with pool.acquire() as conn:
        async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as cli:
            seeded = list(dict.fromkeys(seeds + BRAND_SEEDS))
            random.shuffle(seeded)
            for q in seeded:
                chans = await live_find_channels(cli, q, limit=120)
                await live_backfill_messages(cli, conn, chans, days=365, per_channel=per_channel)

async def crawler_loop():
    seeds = [
        # RU
        "казино", "онлайн казино", "фриспины", "бесплатные вращения",
        "бездепозитный бонус", "промокод казино", "бонус казино", "слоты",
        "рулетка", "блэкджек", "покер", "арбитраж трафика", "партнерка казино", "партнёрка казино",
        # EN
        "casino", "gambling", "free spins", "bonus code", "no deposit bonus",
        "slot machines", "slots", "roulette", "blackjack", "poker",
        "affiliate marketing", "affiliate casino", "cpa gambling", "revshare", "hybrid deal"
    ]
    while True:
        try:
            logging.info("Crawler: cycle start")
            await crawl_once(seeds, per_channel=400)
            logging.info("Crawler: cycle end")
        except Exception as e:
            logging.error(f"Crawler error: {e}")
        await asyncio.sleep(60*30)

# ---------------- Core: DB search helpers ----------------
def _diversify(items: list[dict], max_per_channel: int = 3) -> list[dict]:
    seen = {}
    out = []
    for it in items:
        ch = it.get("chat_username") or it.get("chat")
        c = seen.get(ch, 0)
        if c < max_per_channel:
            out.append(it)
            seen[ch] = c + 1
    return out

async def db_search_messages(conn, q2: str, chat: Optional[str], only_public: bool,
                             since: datetime, toks: list[str],
                             only_promo: bool, no_spam: bool,
                             limit: int, offset: int, max_per_channel: int):
    rows = await conn.fetch(r"""
    WITH m2 AS (
      SELECT m.chat_id, m.message_id, m.date, m.text,
             m.views, m.forwards,
             c.username, c.title,
             lower(regexp_replace(unaccent(coalesce(m.text,'')), '\s+', ' ', 'g')) AS norm
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
    ),
    scored AS (
      SELECT m2.*,
             ts_rank_cd(to_tsvector('simple', m2.norm), plainto_tsquery('simple', $1)) * 0.55
               + similarity(lower(coalesce(m2.text,'')), lower($1)) * 0.45
               AS ft_score,
             md5(m2.norm) AS norm_hash
      FROM m2
    )
    SELECT DISTINCT ON (chat_id, norm_hash)
           chat_id, username, title, message_id, date, text, views, forwards, ft_score
    FROM scored
    ORDER BY chat_id, norm_hash, date DESC
    LIMIT 1500
    """, q2, since, chat, only_public, toks)

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
            "message_id": r["message_id"],
            "date": r["date"].isoformat(),
            "snippet": (txt[:280] + "…") if len(txt) > 280 else txt,
            "score": score
        })

    items.sort(key=lambda x: x["score"], reverse=True)
    items = _diversify(items, max_per_channel=max_per_channel)
    total = len(items)
    items = items[offset:offset+limit]
    return {"total": total, "items": items}

async def db_search_chats(conn, q2: str, only_public: bool, since: datetime, toks: list[str],
                          only_promo: bool, no_spam: bool, limit: int):
    rows = await conn.fetch(r"""
    WITH cand AS (
      SELECT m.chat_id, m.date, m.text, m.views, m.forwards
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
            SELECT 1 FROM unnest($3::text[]) AS t
            WHERE lower(coalesce(m.text,'')) ILIKE ('%'||t||'%')
          )
        )
    ),
    agg AS (
      SELECT c.chat_id, c.username, c.title,
             count(*) AS hits,
             max(cand.date) AS last_post,
             sum(cand.views) AS views_sum,
             sum(cand.forwards) AS fw_sum
      FROM cand
      JOIN channels c ON c.chat_id=cand.chat_id
      GROUP BY c.chat_id, c.username, c.title
    )
    SELECT *
    FROM agg
    WHERE ($4::bool IS FALSE OR username IS NOT NULL)
    ORDER BY last_post DESC
    LIMIT 800
    """, q2, since, toks, only_public)

    now = datetime.now(timezone.utc)
    out = []
    for r in rows:
        ch_url, _ = make_links(r["username"], None)
        score = float(r["hits"]) * 0.45 + recency_weight(r["last_post"], now) * 0.35 + log1p(r["views_sum"]) * 0.12 + log1p(r["fw_sum"]) * 0.08
        out.append({
            "chat": r["username"] or r["title"],
            "chat_username": r["username"],
            "channel_url": ch_url,
            "title": r["title"],
            "last_post": r["last_post"].isoformat() if r["last_post"] else None,
            "hits": r["hits"],
            "score": score
        })

    if only_promo or no_spam:
        filtered = []
        for ch in out:
            if not ch["chat_username"]:
                continue
            ch_id = await conn.fetchval("SELECT chat_id FROM channels WHERE username=$1", ch["chat_username"])
            if not ch_id:
                continue
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

# ---------------- API endpoints ----------------
@app.get("/status")
async def status():
    pool = await db()
    async with pool.acquire() as conn:
        ch_cnt = await conn.fetchval("SELECT count(*) FROM channels")
        msg_cnt = await conn.fetchval("SELECT count(*) FROM messages")
        last_dt = await conn.fetchval("SELECT max(date) FROM messages")
    return {"channels": ch_cnt, "messages": msg_cnt, "last_message_at": last_dt.isoformat() if last_dt else None}

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
    max_per_channel: int = 3,
    live: bool = True,
):
    q2 = expand_query(q)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    toks = tokens_for_like(q2)

    pool = await db()
    async with pool.acquire() as conn:
        if live:
            try:
                async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as cli:
                    chans = await live_find_channels(cli, q, limit=80)
                    if not chans:
                        widened = list(dict.fromkeys([q] + [q2] + BRAND_SEEDS))
                        random.shuffle(widened)
                        for wq in widened[:4]:
                            cs = await live_find_channels(cli, wq, limit=40)
                            chans += cs
                            if len(chans) >= 80:
                                break
                    if chans:
                        await live_backfill_messages(cli, conn, chans[:80], days=days, per_channel=220)
            except Exception as e:
                logging.warning(f"Live phase failed: {e}")

        return JSONResponse(await db_search_messages(
            conn, q2, chat, only_public, since, toks, only_promo, no_spam,
            limit, offset, max_per_channel
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
):
    q2 = expand_query(q)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    toks = tokens_for_like(q2)

    pool = await db()
    async with pool.acquire() as conn:
        live_list = []
        if live:
            try:
                async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as cli:
                    chans = await live_find_channels(cli, q, limit=100)
                    if chans:
                        await live_backfill_messages(cli, conn, chans[:100], days=days, per_channel=200)
                        for ch in chans:
                            ch_url, _ = make_links(getattr(ch,"username",None), None)
                            live_list.append({
                                "chat": getattr(ch,"username",None) or getattr(ch,"title",None),
                                "chat_username": getattr(ch,"username",None),
                                "channel_url": ch_url,
                                "title": getattr(ch,"title",None),
                                "last_post": None,
                                "hits": 0,
                                "score": 0.0
                            })
            except Exception as e:
                logging.warning(f"Live channels failed: {e}")

        res = await db_search_chats(conn, q2, only_public, since, toks, only_promo, no_spam, limit)
        if res["total"] < limit and live_list:
            have = set((it["chat_username"], it["title"]) for it in res["items"])
            for it in live_list:
                key = (it["chat_username"], it["title"])
                if key not in have:
                    res["items"].append(it)
                    have.add(key)
                if len(res["items"]) >= limit:
                    break
            res["total"] = len(res["items"])
        return JSONResponse(res)

# ----------- ENTRYPOINT -----------
async def check_session():
    async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as cli:
        me = await cli.get_me()
        logging.info(f"MTProto OK. Logged in as: {me.first_name} (bot={getattr(me,'bot',False)})")

async def main():
    import uvicorn
    loop = asyncio.get_running_loop()
    await check_session()
    if RUN_CRAWLER:
        logging.info("Starting crawler loop…")
        loop.create_task(crawler_loop())
    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.getenv("PORT","8000")))
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())