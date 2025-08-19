# bot.py
# deps: aiogram==3.7.0 httpx
import os, sys
if os.getenv("ROLE") and os.getenv("ROLE") != "bot":
    sys.exit("BOT: ROLE != bot → выходим")

import re, asyncio, logging, httpx
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_BASE  = os.getenv("SEARCH_API_URL", "http://localhost:8000")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔍 Поиск сообщений")],
        [KeyboardButton(text="📣 Поиск каналов")],
        [KeyboardButton(text="ℹ️ Помощь")]
    ],
    resize_keyboard=True
)

def esc(s: str) -> str:
    return re.sub(r"[<>&]", lambda m: {"<":"&lt;",">":"&gt;","&":"&amp;"}[m.group(0)], s or "")

def parse_days(s: str, default=60):
    m = re.search(r"days:(\d+)", s)
    return int(m.group(1)) if m else default

@dp.message(Command("start"))
async def start(m: types.Message):
    await m.answer(
        "👋 Привет! Пиши запрос (можно добавить <code>days:90</code>)\n"
        "или используй кнопки ниже.",
        reply_markup=main_kb
    )

@dp.message(Command("help"))
async def help_cmd(m: types.Message):
    await m.answer(
        "Примеры:\n"
        "• <code>бесплатные фриспины days:60</code>\n"
        "• <code>промокод казино days:120</code>\n"
        "• <code>affiliate marketing days:150</code>\n\n"
        "Если ничего не найдётся — я попробую расширенный поиск автоматически."
    )

async def api_messages(params):
    async with httpx.AsyncClient(timeout=35) as cli:
        r = await cli.get(f"{API_BASE}/search_messages", params=params)
        r.raise_for_status()
        return r.json().get("items", [])

async def api_chats(params):
    async with httpx.AsyncClient(timeout=35) as cli:
        r = await cli.get(f"{API_BASE}/search_chats", params=params)
        r.raise_for_status()
        return r.json().get("items", [])

@dp.message(F.text == "🔍 Поиск сообщений")
async def ask_msg(m: types.Message):
    await m.answer("Введи запрос. Напр.: <code>фриспины days:60</code>")

@dp.message(F.text == "📣 Поиск каналов")
async def ask_ch(m: types.Message):
    await m.answer("Введи запрос. Напр.: <code>бонус казино days:120</code>")

@dp.message(F.text == "ℹ️ Помощь")
async def show_help(m: types.Message):
    await help_cmd(m)

@dp.message()
async def handle(m: types.Message):
    txt = (m.text or "").strip()
    if not txt:
        return

    days = parse_days(txt, 60)
    query = re.sub(r"days:\d+", "", txt).strip()

    # 1) узкий поиск (строгие фильтры)
    p1 = dict(q=query, days=days, only_promo="true", only_public="true", no_spam="true", limit=6)
    posts = await api_messages(p1)

    if not posts:
        # 2) авто-расширение: больше дней и мягче фильтры
        p2 = dict(q=query, days=max(days, 365), only_promo="false", only_public="false", no_spam="false", limit=8)
        posts = await api_messages(p2)

    if posts:
        for it in posts:
            title = it["chat"] or it.get("chat_username") or "channel"
            date  = it["date"][:10]
            snippet = esc(it["snippet"])
            btn = None
            if it.get("message_url"):
                btn = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔗 Открыть пост", url=it["message_url"])
                ]])
            await m.answer(f"🧭 <b>{esc(title)}</b>  <i>{esc(date)}</i>\n{snippet}", reply_markup=btn)
        return

    # если сообщений нет — пробуем каналы
    c1 = dict(q=query, days=days, only_promo="true", only_public="true", no_spam="true", limit=8)
    chans = await api_chats(c1)
    if not chans:
        c2 = dict(q=query, days=max(days, 365), only_promo="false", only_public="false", no_spam="false", limit=10)
        chans = await api_chats(c2)

    if chans:
        for ch in chans:
            title = ch["title"] or ch["chat"] or "channel"
            last = ch["last_post"][:10] if ch["last_post"] else "—"
            url  = ch.get("channel_url")
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📣 Перейти в канал", url=url)]]) if url else None
            await m.answer(f"📣 <b>{esc(title)}</b>\nпосл. пост: <i>{esc(last)}</i>\nсообщений: {ch['hits']}", reply_markup=kb)
        return

    await m.answer("❌ Ничего не найдено. Попробуй переформулировать запрос (или добавь <code>days:365</code>).")