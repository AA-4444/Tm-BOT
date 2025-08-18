# server.py
# pip install fastapi uvicorn[standard] telethon asyncpg python-dotenv

import os, asyncio, re, math, logging
from datetime import timezone, datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import asyncpg
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel

# ----- Логирование -----
logging.basicConfig(level=logging.INFO)

# ----- Конфиг -----
API_ID  = int(os.getenv("API_ID","0"))
API_HASH = os.getenv("API_HASH","")
STRING_SESSION = os.getenv("TELEGRAM_STRING_SESSION","")
PG_DSN = os.getenv("DATABASE_URL","postgresql://postgres:postgres@localhost:5432/postgres")

app = FastAPI()
_db_pool: asyncpg.Pool | None = None

# ----- SQL -----
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

CREATE INDEX IF NOT EXISTS ix_msg_tsv ON messages
USING GIN (to_tsvector('russian', coalesce(text,'')));
CREATE INDEX IF NOT EXISTS ix_msg_date ON messages(date DESC);
CREATE INDEX IF NOT EXISTS ix_chan_name ON channels USING GIN (to_tsvector('russian', coalesce(title,'')));
"""

async def db():
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
        async with _db_pool.acquire() as c:
            await c.execute(DDL)
    return _db_pool

# ---------- УТИЛИТЫ ПОИСКА ----------
SYNONYMS = {
    r"\bфри[-\s]?спин(ы|ов|а)?\b": ["free spin", "free spins", "фриспин", "спины"],
    r"\bбездеп(озит(ный|а)?)?\b": ["без депозита", "no deposit", "бездепозитный", "ND bonus"],
    r"\bбонус(ы|ов)?\b": ["bonus", "бонус код", "промокод", "promo code", "промо-код"],
    r"\bфриспины\b": ["free spins","спины"],
    r"\bказино\b": ["casino", "онлайн казино"],
}

def expand_query(q: str) -> str:
    ql = q.lower()
    extra = []
    for pat, syns in SYNONYMS.items():
        if re.search(pat, ql):
            extra += syns
    if extra:
        q = q + " " + " ".join(set(extra))
    return q

PROMO_POS = re.compile(r"(бонус|free\s*spins?|фри[-\s]?спин|промо-?код|промокод|бездеп(озит)|депозит\s*бонус|%|крут(и|ить)\s*спины|акци(я|и)|розыгрыш|welcome)", re.I)
PROMO_NEG = re.compile(r"(мем|шутк|юмор|сарказм|ирони|мемы|прикол)", re.I)

def is_promotional(text: str) -> bool:
    if not text: return False
    t = text.lower()
    return bool(PROMO_POS.search(t)) and not bool(PROMO_NEG.search(t))

def recency_weight(date: datetime, now: datetime) -> float:
    age_days = max(0.0, (now - date).total_seconds() / 86400.0)
    return math.exp(-age_days / 30.0)

# ---------- КРАУЛЕР ----------
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

async def discover(client, query: str, limit_chats=40):
    res = await client(SearchRequest(q=query, limit=limit_chats))
    return [c for c in res.chats if isinstance(c, Channel) and getattr(c, 'username', None)]

async def crawl_once(seeds: list[str], limit_msgs: int = 1000):
    pool = await db()
    async with pool.acquire() as conn:
        async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as cli:
            seen = set()
            for q in seeds:
                try:
                    chats = await discover(cli, q, limit_chats=40)
                except FloodWaitError as e:
                    logging.warning(f"FloodWait {e.seconds}s")
                    await asyncio.sleep(e.seconds + 5)
                    continue

                for ch in chats:
                    if ch.id in seen: 
                        continue
                    seen.add(ch.id)

                    ent = await cli.get_entity(ch)
                    meta = {
                        "id": ent.id,
                        "username": getattr(ent, "username", None),
                        "title": getattr(ent, "title", None),
                        "type": "channel" if getattr(ent, "broadcast", False) else "supergroup",
                    }
                    await upsert_channel(conn, meta["id"], meta["username"], meta["title"], meta["type"])

                    async for msg in cli.iter_messages(ent, limit=limit_msgs):
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

async def crawler_loop():
    seeds = [
        "casino", "казино", "free spins", "фриспины",
        "бездепозитный бонус", "промокод казино", "азартные игры", "беттинг"
    ]
    while True:
        try:
            logging.info("Crawler: starting cycle")
            await crawl_once(seeds, limit_msgs=800)
            logging.info("Crawler: done cycle")
        except Exception as e:
            logging.error(f"Crawler error: {e}")
        await asyncio.sleep(60*30)

# ---------- API ----------
@app.get("/search_messages")
async def search_messages(q: str = Query(..., min_length=2), chat: Optional[str] = None,
                          days: int = 90, only_promo: bool = True,
                          limit: int = 20, offset: int = 0):
    q2 = expand_query(q)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
        SELECT m.chat_id, c.username, c.title, m.message_id, m.date, m.text,
               ts_rank_cd(to_tsvector('russian', coalesce(m.text,'')),
                          plainto_tsquery('russian', $1)) AS ft_score
        FROM messages m
        JOIN channels c USING(chat_id)
        WHERE m.date >= $2
          AND to_tsvector('russian', coalesce(m.text,'')) @@ plainto_tsquery('russian', $1)
          AND ($3::text IS NULL OR lower(c.username)=lower($3))
        ORDER BY m.date DESC
        LIMIT 400
        """, q2, since, chat)

    now = datetime.now(timezone.utc)
    items = []
    for r in rows:
        txt = r["text"] or ""
        if only_promo and not is_promotional(txt):
            continue
        w = recency_weight(r["date"], now)
        score = float(r["ft_score"] or 0.0) * 0.7 + w * 0.3
        items.append({
            "chat": r["username"] or r["title"],
            "chat_username": r["username"],
            "message_id": r["message_id"],
            "date": r["date"].isoformat(),
            "snippet": (txt[:280] + "…") if len(txt) > 280 else txt,
            "score": score
        })
    items.sort(key=lambda x: x["score"], reverse=True)
    return JSONResponse({"total": len(items), "items": items[offset:offset+limit]})

@app.get("/search_chats")
async def search_chats(q: str = Query(..., min_length=2), days: int = 90,
                       only_promo: bool = True, limit: int = 15):
    q2 = expand_query(q)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
        WITH cand AS (
          SELECT m.chat_id, m.date, m.text,
                 ts_rank_cd(to_tsvector('russian', coalesce(m.text,'')),
                            plainto_tsquery('russian', $1)) AS ft_score
          FROM messages m
          WHERE m.date >= $2
            AND to_tsvector('russian', coalesce(m.text,'')) @@ plainto_tsquery('russian', $1)
        )
        SELECT c.chat_id, c.username, c.title,
               count(*) AS hits,
               max(date) AS last_post
        FROM cand
        JOIN channels c ON c.chat_id=cand.chat_id
        GROUP BY c.chat_id, c.username, c.title
        ORDER BY last_post DESC
        LIMIT 300
        """, q2, since)

    now = datetime.now(timezone.utc)
    out = []
    for r in rows:
        w = recency_weight(r["last_post"], now)
        base = float(r["hits"])
        score = base * 0.6 + w * 0.4
        out.append({
            "chat": r["username"] or r["title"],
            "chat_username": r["username"],
            "title": r["title"],
            "last_post": r["last_post"].isoformat() if r["last_post"] else None,
            "hits": r["hits"],
            "score": score
        })
    out.sort(key=lambda x: x["score"], reverse=True)
    return JSONResponse({"total": len(out), "items": out[:limit]})

# ----------- ENTRYPOINT -----------
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