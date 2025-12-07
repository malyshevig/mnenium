
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from pydub import AudioSegment
from publisher import Publisher
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

bot_name =  "t.me/im77_chat_bot"

# Токен бота из переменной окружения
BOT_TOKEN = "8535593950:AAHGhZ4mRK7LWWl2Q63-c5iC7aKS0E3gWJ4"

publisher = Publisher ()
data_dir = "/Users/im/data/mnenium"

def export_to_mp3(filename)->str:
    # После скачивания файла
    src_path = Path(data_dir, filename)
    dst_path = Path(data_dir, filename.replace('.ogg', '.mp3'))
    audio = AudioSegment.from_ogg(src_path)

    audio.export(dst_path, format='mp3')
    return str(dst_path)


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик голосовых сообщений"""
    try:
        voice = update.message.voice
        user = update.message.from_user

        # Скачиваем голосовое сообщение
        voice_file = await voice.get_file()
        filename = f"voice_{user.id}_{voice.file_id}.ogg"
        path = Path(data_dir, filename)

        await voice_file.download_to_drive(path)
        filename=export_to_mp3(filename)

        logger.info(f"Получено голосовое сообщение от {user.first_name}. Файл сохранен как {filename}")
        publisher.publish_voice(update, filename)
        # Отправляем подтверждение
        await update.message.reply_text(
            f"✅ Голосовое сообщение сохранено!\n"
            f"Длительность: {voice.duration} сек.\n"
            f"Размер файла: {voice.file_size} байт"
        )

    except Exception as e:
        logger.error(f"Ошибка при обработке голосового сообщения: {e}")
        await update.message.reply_text("❌ Произошла ошибка при обработке сообщения")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    publisher.publish_text(update)

    await update.message.reply_text(
        "Сообщение принято. Спасибо!"
    )


def main():
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()

    # Добавляем обработчики
    application.add_handler(MessageHandler(filters.VOICE, handle_voice))
    application.add_handler(MessageHandler(filters.TEXT, handle_text))

    # Запускаем бота
    application.run_polling()
    logger.info("Бот запущен")


if __name__ == "__main__":
    main()