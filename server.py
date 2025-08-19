# bot.py
# deps: aiogram==3.7.0 httpx

import os, re, asyncio, logging, httpx
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_BASE  = os.getenv("SEARCH_API_URL", "http://localhost:8000")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# --- UI ---
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

# режим «широкого» поиска на 1 запрос
user_wide: dict[int, bool] = {}

# --- Commands ---
@dp.message(Command("start"))
async def start(m: types.Message):
    await m.answer(
        "👋 Привет! Я найду промо-посты и каналы в казино-нише.\n\n"
        "Выбери действие кнопкой ниже или просто напиши запрос.\n"
        "Можно добавить диапазон дней: <code>days:90</code>.",
        reply_markup=main_kb
    )

@dp.message(Command("help"))
async def help_cmd(m: types.Message):
    await m.answer(
        "ℹ️ <b>Примеры запросов</b>:\n"
        "• <code>бесплатные фриспины days:60</code>\n"
        "• <code>промокод казино days:120</code>\n"
        "• <code>affiliate marketing days:150</code>\n\n"
        "Команды:\n"
        "• <b>/wide</b> — расширенный поиск (меньше фильтров) на следующий запрос."
    )

@dp.message(Command("wide"))
async def wide(m: types.Message):
    user_wide[m.from_user.id] = True
    await m.answer("🔎 Включён расширенный поиск на следующий запрос.")

# --- API calls ---
async def api_search_messages(query: str, days: int, wide: bool):
    params = {
        "q": query,
        "days": days,
        "only_promo": "false" if wide else "true",
        "only_public": "true",
        "no_spam": "false" if wide else "true",
        "limit": 6
    }
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.get(f"{API_BASE}/search_messages", params=params)
        r.raise_for_status()
        return r.json().get("items", [])

async def api_search_chats(query: str, days: int, wide: bool):
    params = {
        "q": query,
        "days": days,
        "only_promo": "false" if wide else "true",
        "only_public": "true",
        "no_spam": "false" if wide else "true",
        "limit": 8
    }
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.get(f"{API_BASE}/search_chats", params=params)
        r.raise_for_status()
        return r.json().get("items", [])

# --- Buttons handlers ---
@dp.message(F.text == "🔍 Поиск сообщений")
async def ask_msg_query(m: types.Message):
    await m.answer("Введи запрос для поиска сообщений.\nНапример: <code>фриспины days:60</code>")

@dp.message(F.text == "📣 Поиск каналов")
async def ask_channel_query(m: types.Message):
    await m.answer("Введи запрос для поиска каналов.\nНапример: <code>бонус казино days:120</code>")

@dp.message(F.text == "ℹ️ Помощь")
async def show_help(m: types.Message):
    await help_cmd(m)

# --- Free text: сначала сообщения, если пусто — каналы ---
@dp.message()
async def handle_text(m: types.Message):
    txt = (m.text or "").strip()
    if not txt:
        return

    days = parse_days(txt, default=60)
    query = re.sub(r"days:\d+", "", txt).strip()
    wide = user_wide.pop(m.from_user.id, False)

    # 1) Сообщения
    posts = await api_search_messages(query, days, wide)
    if posts:
        for it in posts:
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

    # 2) Каналы
    chans = await api_search_chats(query, days, wide)
    if chans:
        for ch in chans:
            title = ch["title"] or ch["chat"] or "channel"
            last = ch["last_post"][:10] if ch["last_post"] else "—"
            url  = ch.get("channel_url")
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="📣 Перейти в канал", url=url)]] if url else []
            )
            await m.answer(f"📣 <b>{esc(title)}</b>\nпосл. пост: <i>{esc(last)}</i>\nсообщений: {ch['hits']}", reply_markup=kb)
        return

    await m.answer("❌ Ничего не найдено. Попробуй другой запрос или увеличь <code>days</code>.\n"
                   "Можно включить расширенный режим: /wide")

# --- Entrypoint ---
if __name__ == "__main__":
    async def runner():
        if not BOT_TOKEN or not API_BASE:
            logging.error("❌ BOT_TOKEN/SEARCH_API_URL отсутствуют")
            return
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("✅ Bot polling started")
        await dp.start_polling(bot)
    asyncio.run(runner())