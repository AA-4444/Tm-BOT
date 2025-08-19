# server.py
# deps: fastapi uvicorn[standard] telethon asyncpg python-dotenv
import os, sys
if os.getenv("ROLE") and os.getenv("ROLE") != "server":
    sys.exit("API: ROLE != server → выходим")
    
import os, asyncio, re, math, logging
from datetime import timezone, datetime, timedelta
from typing import Optional, List

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import asyncpg

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel

logging.basicConfig(level=logging.INFO)

API_ID  = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
STRING_SESSION = os.getenv("TELEGRAM_STRING_SESSION", "")
PG_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

app = FastAPI()
_db_pool: Optional[asyncpg.Pool] = None

# ---------- Схема + миграция tsv_all (RU+EN) ----------
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

CREATE INDEX IF NOT EXISTS ix_msg_date ON messages(date DESC);
CREATE INDEX IF NOT EXISTS ix_chan_name ON channels USING GIN (to_tsvector('russian', coalesce(title,'')));
"""

async def db() -> asyncpg.Pool:
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
        async with _db_pool.acquire() as c:
            await c.execute(DDL)
            # миграция RU+EN tsvector + индекс
            await c.execute("""
            ALTER TABLE messages
            ADD COLUMN IF NOT EXISTS tsv_all tsvector;

            UPDATE messages
            SET tsv_all =
              to_tsvector('russian', coalesce(text,'')) ||
              to_tsvector('english', coalesce(text,''))
            WHERE tsv_all IS NULL;

            CREATE INDEX IF NOT EXISTS ix_msg_tsv_all ON messages USING GIN (tsv_all);
            """)
    return _db_pool

# ---------- Утилиты ----------
SYNONYMS = {
    r"\bфри[-\s]?спин(ы|ов|а)?\b": ["free spin", "free spins", "фриспин", "спины", "бесплатные вращения"],
    r"\bбездеп(озит(ный|а)?)?\b": ["без депозита", "no deposit", "бездепозитный", "ND bonus"],
    r"\bбонус(ы|ов)?\b": ["bonus", "бонус код", "промокод", "promo code", "промо-код", "cashback", "кэшбек", "вейджер", "wager"],
    r"\bфриспины\b": ["free spins","спины","бесплатные вращения"],
    r"\bказино\b": ["casino", "онлайн казино", "slots", "слоты"],
    r"\baffiliate\b": ["cpa", "gambling", "арбитраж", "арбитраж трафика", "партнёрка"],
}
COMMON_TYPO = {
    "affilate": "affiliate",
    "affliate": "affiliate",
    "markting": "marketing",
    "kazino": "казино",
}
def fix_typos(q: str) -> str:
    ql = q.lower()
    for bad, good in COMMON_TYPO.items():
        ql = re.sub(rf"\b{bad}\b", good, ql)
    return ql

def expand_query(q: str) -> str:
    ql = fix_typos(q)
    extra: List[str] = []
    for pat, syns in SYNONYMS.items():
        if re.search(pat, ql):
            extra += syns
    if extra:
        ql = ql + " " + " ".join(sorted(set(extra)))
    return ql

PROMO_POS = re.compile(r"(бонус|free\s*spins?|фри[-\s]?спин|промо-?код|промокод|бездеп(озит)|депозит\s*бонус|welcome|кэшбек|cash\s*back)", re.I)
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
    return bool(text) and bool(SPAM_NEG.search(text.lower()))

def recency_weight(date: datetime, now: datetime) -> float:
    age_days = max(0.0, (now - date).total_seconds()/86400.0)
    return math.exp(-age_days/30.0)

def make_links(username: Optional[str], message_id: Optional[int]):
    if not username:
        return None, None
    ch = f"https://t.me/{username}"
    msg = f"{ch}/{message_id}" if message_id else None
    return ch, msg

# ---------- Краулер ----------
async def upsert_channel(conn, chat_id, username, title, typ):
    await conn.execute("""
    INSERT INTO channels(chat_id, username, title, type)
    VALUES($1,$2,$3,$4)
    ON CONFLICT (chat_id) DO UPDATE
      SET username=EXCLUDED.username, title=EXCLUDED.title, type=EXCLUDED.type
    """, chat_id, username, title, typ)

async def upsert_message(conn, row):
    # вставляем сразу RU+EN tsvector
    await conn.execute("""
    INSERT INTO messages(
        chat_id, message_id, date, text, has_media, links, views, forwards, tsv_all
    )
    VALUES(
        $1,$2,$3,$4,$5,$6,$7,$8,
        to_tsvector('russian', coalesce($4,'')) ||
        to_tsvector('english', coalesce($4,''))
    )
    ON CONFLICT (chat_id, message_id) DO NOTHING
    """,
    row["chat_id"], row["message_id"], row["date"], row["text"],
    row["has_media"], row["links"], row["views"], row["forwards"])

async def discover(client, query: str, limit_chats=100):
    res = await client(SearchRequest(q=query, limit=limit_chats))
    return [c for c in res.chats if isinstance(c, Channel) and getattr(c, "username", None)]

async def crawl_once(seeds: List[str], limit_msgs: int = 900):
    pool = await db()
    async with pool.acquire() as conn:
        async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as cli:
            seen = set()
            for q in seeds:
                try:
                    chats = await discover(cli, q, limit_chats=100)
                except FloodWaitError as e:
                    logging.warning(f"FloodWait {e.seconds}s"); await asyncio.sleep(e.seconds + 5); continue

                for ch in chats:
                    if ch.id in seen: 
                        continue
                    seen.add(ch.id)

                    ent = await cli.get_entity(ch)
                    await upsert_channel(conn, ent.id, getattr(ent,"username",None), getattr(ent,"title",None),
                                         "channel" if getattr(ent,"broadcast",False) else "supergroup")

                    async for msg in cli.iter_messages(ent, limit=limit_msgs):
                        text = msg.message or ""
                        await upsert_message(conn, {
                            "chat_id": ent.id,
                            "message_id": msg.id,
                            "date": (msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)),
                            "text": text,
                            "has_media": bool(msg.media),
                            "links": len(re.findall(r"https?://", text)),
                            "views": getattr(msg,"views",0) or 0,
                            "forwards": getattr(msg,"forwards",0) or 0,
                        })

async def crawler_loop():
    seeds = [
        # gambling / casino
        "casino","казино","free spins","фриспины","бесплатные вращения",
        "бездепозитный бонус","бонус казино","промокод казино","слоты","slots",
        # affiliate / cpa
        "affiliate marketing","affiliate casino","cpa gambling","арбитраж трафика","партнерка казино",
    ]
    while True:
        try:
            logging.info("Crawler: starting cycle")
            await crawl_once(seeds, limit_msgs=800)
            logging.info("Crawler: done cycle")
        except Exception as e:
            logging.error(f"Crawler error: {e}")
        await asyncio.sleep(60*30)

# ---------- Endpoints ----------
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
):
    q_fixed = expand_query(q)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))

    pool = await db()
    async with pool.acquire() as conn:
        # FTS (ru+en) + websearch fallback + триграммы на опечатки
        rows = await conn.fetch("""
        WITH m2 AS (
          SELECT m.chat_id, m.message_id, m.date, m.text,
                 c.username, c.title,
                 -- нормализованный текст для триграмм
                 lower(regexp_replace(unaccent(coalesce(m.text,'')), '\s+', ' ', 'g')) AS norm,
                 -- fts совпадения
                 (
                   m.tsv_all @@ (plainto_tsquery('russian',$1) || plainto_tsquery('english',$1)) OR
                   m.tsv_all @@ (websearch_to_tsquery('russian',$1) || websearch_to_tsquery('english',$1))
                 ) AS fts_ok,
                 -- триграммная похожесть
                 similarity(lower(unaccent(coalesce(m.text,''))), lower(unaccent($1))) AS trig
          FROM messages m
          JOIN channels c USING(chat_id)
          WHERE m.date >= $2
            AND ($3::text IS NULL OR lower(c.username)=lower($3))
            AND ($4::bool IS FALSE OR c.username IS NOT NULL)
            AND (
              -- либо FTS, либо очень похожий текст (опечатки)
              (
                m.tsv_all @@ (plainto_tsquery('russian',$1) || plainto_tsquery('english',$1)) OR
                m.tsv_all @@ (websearch_to_tsquery('russian',$1) || websearch_to_tsquery('english',$1))
              )
              OR similarity(lower(unaccent(coalesce(m.text,''))), lower(unaccent($1))) >= 0.25
            )
        ),
        scored AS (
          SELECT m2.*,
                 -- финальный скор: FTS ранг + свежесть + триграммы
                 ts_rank_cd(
                   to_tsvector('simple', m2.norm),
                   plainto_tsquery('simple', $1)
                 )*0.5
                 + m2.trig*0.3
                 + EXTRACT(EPOCH FROM (now() at time zone 'utc' - m2.date))*(-1)/86400.0*0.2
                   AS ft_score,
                 md5(m2.norm) AS norm_hash
          FROM m2
        )
        SELECT DISTINCT ON (chat_id, norm_hash)
               chat_id, username, title, message_id, date, text, ft_score
        FROM scored
        ORDER BY chat_id, norm_hash, ft_score DESC, date DESC
        LIMIT 700
        """, q_fixed, since, chat, only_public)

    now = datetime.now(timezone.utc)
    items = []
    for r in rows:
        txt = r["text"] or ""
        if no_spam and is_spammy(txt):
            continue
        if only_promo and not is_promotional(txt):
            continue
        w = recency_weight(r["date"], now)
        score = float(r["ft_score"] or 0.0) * 0.7 + w * 0.3
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

    # авто-добор: если мало — докраулим по самому запросу (фоном)
    if len(items) < 5:
        try:
            async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as cli:
                _ = await discover(cli, q_fixed, limit_chats=60)
        except Exception:
            pass
        asyncio.create_task(crawl_once([q_fixed], limit_msgs=400))

    items.sort(key=lambda x: x["score"], reverse=True)
    total = len(items)
    items = items[offset:offset+limit]
    return JSONResponse({"total": total, "items": items})

@app.get("/search_chats")
async def search_chats(
    q: str = Query(..., min_length=2),
    days: int = 90,
    only_promo: bool = True,
    only_public: bool = True,
    no_spam: bool = True,
    limit: int = 15
):
    q_fixed = expand_query(q)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))

    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
        WITH cand AS (
          SELECT m.chat_id, m.date, m.text,
                 (
                   m.tsv_all @@ (plainto_tsquery('russian',$1) || plainto_tsquery('english',$1)) OR
                   m.tsv_all @@ (websearch_to_tsquery('russian',$1) || websearch_to_tsquery('english',$1))
                 ) AS fts_ok,
                 similarity(lower(unaccent(coalesce(m.text,''))), lower(unaccent($1))) AS trig
          FROM messages m
          WHERE m.date >= $2
            AND (
              fts_ok OR trig >= 0.25
            )
        ),
        agg AS (
          SELECT c.chat_id, c.username, c.title,
                 count(*) AS hits,
                 max(cand.date) AS last_post
          FROM cand
          JOIN channels c ON c.chat_id=cand.chat_id
          GROUP BY c.chat_id, c.username, c.title
        )
        SELECT * FROM agg
        WHERE ($3::bool IS FALSE OR username IS NOT NULL)
        ORDER BY last_post DESC
        LIMIT 400
        """, q_fixed, since, only_public)

    now = datetime.now(timezone.utc)
    out = []
    for r in rows:
        ch_url, _ = make_links(r["username"], None)
        out.append({
            "chat": r["username"] or r["title"],
            "chat_username": r["username"],
            "channel_url": ch_url,
            "title": r["title"],
            "last_post": r["last_post"].isoformat() if r["last_post"] else None,
            "hits": r["hits"],
            "score": float(r["hits"]) * 0.6 + recency_weight(r["last_post"], now) * 0.4
        })

    # лёгкая проверка промо/спама по последним постам
    if only_promo or no_spam:
        filtered = []
        async with pool.acquire() as conn2:
            for ch in out:
                if not ch["chat_username"]:
                    continue
                ch_id = await conn2.fetchval("SELECT chat_id FROM channels WHERE username=$1", ch["chat_username"])
                msgs = await conn2.fetch("""
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
    return JSONResponse({"total": len(out), "items": out[:limit]})

# ---------- Entrypoint ----------
async def check_session():
    async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as cli:
        me = await cli.get_me()
        logging.info(f"MTProto OK. Logged in as: {me.first_name} (bot={getattr(me,'bot',False)})")

async def main():
    import uvicorn
    loop = asyncio.get_running_loop()
    await check_session()
    logging.info("Starting crawler loop…")
    loop.create_task(crawler_loop())
    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.getenv("PORT","8000")))
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())