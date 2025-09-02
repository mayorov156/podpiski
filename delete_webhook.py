import asyncio
from aiogram import Bot
from config import API_TOKEN

async def delete_bot_webhook():
    bot = Bot(token=API_TOKEN)
    try:
        await bot.delete_webhook()
        print("Вебхук успешно удален.")
    except Exception as e:
        print(f"Ошибка при удалении вебхука: {e}")
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(delete_bot_webhook()) 