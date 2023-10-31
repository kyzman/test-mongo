import contextlib, re, json

from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from config import BOT_TOKEN
import asyncio

from mongodata import get_mongo_mycollection, get_db_data

bot = Bot(BOT_TOKEN, parse_mode='HTML')
dp = Dispatcher(bot=bot)

my_collection = get_mongo_mycollection("mongodb://127.0.0.1:27017")


@dp.message()
async def echo_answer(msg: types.Message):
    pattern = re.compile(r"""^\s{0,}[{]\s{0,}[\"']dt_from[\"']:\s{0,}[\"']\d{4}[-]\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[\"'],\s{0,}[\"']dt_upto[\"']:\s{0,}[\"']\d{4}[-]\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[\"'],\s{0,}[\"']group_type[\"']:\s{0,}[\"']\w+[\"']\s{0,}[}]\s{0,}$""")
    if not pattern.match(msg.text):
        await msg.answer('Введите корректную информацию!')
        return
    payload = json.loads(msg.text)
    print(payload)
    output = get_db_data(datetime.fromisoformat(payload["dt_from"]), datetime.fromisoformat(payload["dt_upto"]),
                             payload["group_type"], my_collection)
    print(output)
    await msg.answer(str(output))



if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))