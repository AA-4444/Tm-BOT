# bot.py
# pip install aiogram==3.7.0 httpx

import os, re, asyncio, logging, httpx
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_BASE = os.getenv("SEARCH_API_URL", "http://localhost:8000")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# ---------- утилиты ----------
def esc(s: str) -> str:
    return re.sub(r"[<>&]", lambda m: {"<":"&lt;",">":"&gt;","&":"&amp;"}[m.group(0)], s or "")

def parse_days(s: str, default=60):
    m = re.search(r"days:(\d+)", s)
    return int(m.group(1)) if m else default

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔍 Поиск сообщений")],
        [KeyboardButton(text="📣 Поиск каналов")],
        [KeyboardButton(text="ℹ️ Помощь")]
    ],
    resize_keyboard=True
)

# ---------- команды ----------
@dp.message(Command("start"))
async def start(m: types.Message):
    await m.answer(
        "👋 Привет! Я помогу искать промо-посты и каналы в казино-нишe.\n\n"
        "Выбери действие кнопкой ниже 👇",
        reply_markup=main_kb
    )

@dp.message(Command("help"))
async def help_cmd(m: types.Message):
    await m.answer(
        "ℹ️ <b>Как пользоваться:</b>\n\n"
        "• Нажми <b>🔍 Поиск сообщений</b> и введи ключевые слова\n"
        "  (например: <i>бесплатные фриспины</i>).\n\n"
        "• Нажми <b>📣 Поиск каналов</b> и введи тематику\n"
        "  (например: <i>казино бонус</i>).\n\n"
        "Дополнительно можно указать диапазон дней:\n"
        "<code>days:90</code> — искать посты за последние 90 дней."
    )

# ---------- логика поиска ----------
async def search_messages(query: str, days: int = 60):
    params = {"q": query, "days": days, "only_promo": "true", "only_public": "true", "no_spam": "true", "limit": 6}
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.get(f"{API_BASE}/search_messages", params=params)
        r.raise_for_status()
        return r.json().get("items", [])

async def search_chats(query: str, days: int = 90):
    params = {"q": query, "days": days, "only_promo": "true", "only_public": "true", "no_spam": "true", "limit": 8}
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.get(f"{API_BASE}/search_chats", params=params)
        r.raise_for_status()
        return r.json().get("items", [])

# ---------- обработчики кнопок ----------
@dp.message(F.text == "🔍 Поиск сообщений")
async def ask_msg_query(m: types.Message):
    await m.answer("Введи запрос для поиска сообщений.\n\nНапример: <code>фриспины days:60</code>")

@dp.message(F.text == "📣 Поиск каналов")
async def ask_channel_query(m: types.Message):
    await m.answer("Введи запрос для поиска каналов.\n\nНапример: <code>бонус казино days:120</code>")

@dp.message(F.text == "ℹ️ Помощь")
async def show_help(m: types.Message):
    await help_cmd(m)

# ---------- обработка «свободного текста» ----------
@dp.message()
async def handle_text(m: types.Message):
    txt = m.text.strip()
    if not txt:
        return
    days = parse_days(txt, default=60)
    query = re.sub(r"days:\d+","",txt).strip()

    # пробуем сначала поиск сообщений
    items = await search_messages(query, days)
    if items:
        lines = []
        for it in items:
            title = it["chat"] or it.get("chat_username") or "channel"
            date  = it["date"][:10]
            ch_url, msg_url = it.get("channel_url"), it.get("message_url")
            snippet = esc(it["snippet"])
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔗 Открыть пост", url=msg_url)]] if msg_url else []
            )
            header = f"🧭 <b>{esc(title)}</b>  <i>{esc(date)}</i>"
            await m.answer(f"{header}\n{snippet}", reply_markup=kb)
        return

    # если нет сообщений → пробуем поиск каналов
    channels = await search_chats(query, days)
    if channels:
        for ch in channels:
            title = ch["title"] or ch["chat"] or "channel"
            last = ch["last_post"][:10] if ch["last_post"] else "—"
            url = ch.get("channel_url")
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="📣 Перейти в канал", url=url)]] if url else []
            )
            await m.answer(f"📣 <b>{esc(title)}</b>\nпосл. пост: <i>{esc(last)}</i>\nсообщений: {ch['hits']}", reply_markup=kb)
        return

    await m.answer("❌ Ничего не найдено. Попробуй другой запрос или увеличь days.")

# ---------- entrypoint ----------
if __name__ == "__main__":
    async def runner():
        if not BOT_TOKEN:
            logging.error("❌ BOT_TOKEN отсутствует")
            return
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("✅ Bot polling started")
        await dp.start_polling(bot)
    asyncio.run(runner())