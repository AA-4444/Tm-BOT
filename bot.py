# bot.py
# pip install aiogram==3.7.0 httpx
import os, re, asyncio, logging, httpx, time
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

logging.basicConfig(level=logging.INFO)
print("🔧 Booting bot.py...")  # очень ранний лог в stdout

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_BASE  = os.getenv("SEARCH_API_URL", "")

# Ранние проверки переменных с явными логами
if not BOT_TOKEN:
    print("❌ BOT_TOKEN is missing in environment")
if not API_BASE:
    print("❌ SEARCH_API_URL is missing in environment")

def esc(s: str) -> str:
    return re.sub(r"[<>&]", lambda m: {"<":"&lt;",">":"&gt;","&":"&amp;"}[m.group(0)], s or "")

def parse_days(s: str, default=60):
    m = re.search(r"days:(\d+)", s)
    return int(m.group(1)) if m else default

# Если нет конфига — не выходим молча, а оставляем процесс живым с логом
if not BOT_TOKEN or not API_BASE:
    print("⏸ Bot not configured. Waiting here so you can see logs...")
    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        pass
    raise SystemExit(1)

# Инициализация бота под aiogram 3.7.0+
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

@dp.message(Command("start"))
async def start(m: types.Message):
    await m.answer(
        "🔍 Поиск по казино-нишe.\n"
        "Команды:\n"
        "• /search <запрос> [days:60]\n"
        "• /channels <запрос> [days:60]"
    )

@dp.message(Command("search"))
async def search_cmd(m: types.Message):
    parts = m.text.split(maxsplit=1)
    if len(parts) == 1:
        return await m.answer("Пример: /search бесплатные фриспины days:60")
    query = parts[1]
    days = parse_days(query)
    query = re.sub(r"days:\d+","",query).strip()

    params = {"q": query, "days": days, "only_promo": "true", "limit": 8}
    async with httpx.AsyncClient(timeout=25) as cli:
        r = await cli.get(f"{API_BASE}/search_messages", params=params)
        r.raise_for_status()
        data = r.json()

    if not data.get("items"):
        return await m.answer("❌ Ничего свежего не найдено. Попробуй изменить запрос или увеличить days.")

    lines = []
    for it in data["items"]:
        title = it["chat"] or "unknown"
        date  = it["date"][:10]
        snippet = it["snippet"]
        lines.append(f"🧭 <b>{esc(title)}</b>  <i>{esc(date)}</i>\n{esc(snippet)}")

    await m.answer("\n\n".join(lines))

@dp.message(Command("channels"))
async def channels_cmd(m: types.Message):
    parts = m.text.split(maxsplit=1)
    if len(parts) == 1:
        return await m.answer("Пример: /channels бездепозитный бонус days:90")
    query = parts[1]
    days = parse_days(query, default=90)
    query = re.sub(r"days:\d+","",query).strip()

    params = {"q": query, "days": days, "only_promo": "true", "limit": 10}
    async with httpx.AsyncClient(timeout=25) as cli:
        r = await cli.get(f"{API_BASE}/search_chats", params=params)
        r.raise_for_status()
        data = r.json()

    if not data.get("items"):
        return await m.answer("❌ Каналы не найдены. Попробуй другой запрос или увеличь days.")

    lines = []
    for ch in data["items"]:
        name = ch["chat_username"] or ch["title"] or "channel"
        last = ch["last_post"][:10] if ch["last_post"] else "—"
        lines.append(f"📣 <b>{esc(name)}</b>  (посл. пост: {esc(last)})  · hits: {ch['hits']}")

    await m.answer("\n".join(lines))

if __name__ == "__main__":
    async def runner():
        # На всякий случай выключим вебхук (если вдруг был)
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("✅ Bot polling started")
        await dp.start_polling(bot)

    asyncio.run(runner())