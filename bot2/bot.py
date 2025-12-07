# distributed_bot.py
import asyncio
from pathlib import Path

import aiohttp
import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.base import BaseStorage
from aiogram.fsm.context import FSMContext
from pydub import AudioSegment

from bot2 import publisher2
from etcd_leader import LongPollingLeaderElection

data_dir = "/Users/im/data/mnenium"
logger = logging.getLogger(__name__)

publisher = publisher2.Publisher ()

def export_to_mp3(filename)->str:
    # После скачивания файла
    src_path = Path(data_dir, filename)
    dst_path = Path(data_dir, filename.replace('.ogg', '.mp3'))
    audio = AudioSegment.from_ogg(src_path)

    audio.export(dst_path, format='mp3')
    return str(dst_path)

class DistributedLongPollingBot:
    """
    Высокодоступный Telegram бот с long polling через etcd
    """
    
    def __init__(
        self,
        token: str,
        etcd_hosts: list,
        polling_timeout: int = 30,
        polling_limit: int = 100,
        allowed_updates: Optional[List[str]] = None
    ):
        # Telegram Bot
        self.token = token
        self.bot = Bot(token=token)
        
        # etcd и Leader Election
        self.leader_election = LongPollingLeaderElection(
            etcd_hosts=etcd_hosts,
            bot_token=token,
            lease_ttl=20
        )
        
        # Настройка polling
        self.polling_timeout = polling_timeout
        self.polling_limit = polling_limit
        self.allowed_updates = allowed_updates or [
            "message", "callback_query", "inline_query"
        ]
        
        # Состояние
        self.is_running = False
        self.polling_task: Optional[asyncio.Task] = None
        self.offset = 0
        
        # HTTP сессия для прямых запросов к API
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Обработчики
        self.handlers = {}
        
        # Статистика
        self.stats = {
            'start_time': datetime.now(),
            'updates_received': 0,
            'updates_processed': 0,
            'errors': 0,
            'last_update_time': None
        }
        
        # Настройка callbacks
        self.leader_election.on_leader_elected = self._on_leader_elected
        self.leader_election.on_leader_lost = self._on_leader_lost
        
        # Регистрация команд
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Регистрация обработчиков по умолчанию"""
        async def cmd_start(message: types.Message):
            cluster_status = self.leader_election.get_cluster_status()
            status_text = (
                f"🤖 *Высокодоступный бот*\n\n"
                f"• Инстанс: `{self.leader_election.instance_id[:8]}`\n"
                f"• Лидер: `{cluster_status.get('current_leader', 'unknown')[:8]}`\n"
                f"• Активные инстансы: {len(cluster_status.get('active_instances', []))}\n"
                f"• Offset: {self.offset}\n"
                f"• Время работы: {datetime.now() - self.stats['start_time']}"
            )
            await message.answer(status_text, parse_mode="Markdown")
        
        async def cmd_status(message: types.Message):
            status = self._get_bot_status()
            await message.answer(
                f"📊 *Статус бота*\n\n"
                f"```json\n{json.dumps(status, indent=2, ensure_ascii=False)}\n```",
                parse_mode="Markdown"
            )
        
        async def cmd_ping(message: types.Message):
            await message.answer("🏓 Pong!")
        
        # Сохраняем обработчики
        self.handlers['/start'] = cmd_start
        self.handlers['/status'] = cmd_status
        self.handlers['/ping'] = cmd_ping
    
    async def _on_leader_elected(self):
        """Callback при избрании лидером"""
        logger.info("I am now the leader, starting polling...")
        
        # Создаем HTTP сессию
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.polling_timeout + 10)
        )
        
        # Запускаем polling
        self.polling_task = asyncio.create_task(self._polling_loop())
    
    async def _on_leader_lost(self):
        """Callback при потере лидерства"""
        logger.info("I am no longer the leader, stopping polling...")
        
        # Останавливаем polling
        if self.polling_task and not self.polling_task.done():
            self.polling_task.cancel()
            try:
                await self.polling_task
            except asyncio.CancelledError:
                pass
        
        # Закрываем сессию
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _polling_loop(self):
        """Основной цикл long polling"""
        logger.info("Starting long polling loop")
        
        while self.leader_election.is_leader and self.is_running:
            try:
                # Получаем обновления
                updates = await self._get_updates()
                
                if updates and updates.get('ok'):
                    await self._process_updates(updates['result'])
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Polling error: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(5)
    
    async def _get_updates(self) -> Dict[str, Any]:
        """Получение обновлений через Telegram API"""
        try:
            # Формируем параметры запроса
            params = {
                'offset': self.offset + 1,
                'timeout': self.polling_timeout,
                'limit': self.polling_limit,
                'allowed_updates': json.dumps(self.allowed_updates)
            }
            
            # Отправляем запрос
            url = f"https://api.telegram.org/bot{self.token}/getUpdates"
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logger.error(f"API error: {response.status}")
                    return {'ok': False, 'result': []}
                    
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error: {e}")
            return {'ok': False, 'result': []}
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {'ok': False, 'result': []}
    
    async def _process_updates(self, updates: List[Dict[str, Any]]):
        """Обработка полученных обновлений"""
        if not updates:
            return
        
        # Обновляем статистику
        self.stats['updates_received'] += len(updates)
        self.stats['last_update_time'] = datetime.now()
        
        for update in updates:
            try:
                update_id = update.get('update_id')
                
                # Обновляем offset
                if update_id > self.offset:
                    self.offset = update_id
                    await self.leader_election.save_offset(update_id)
                
                # Обрабатываем update
                await self._process_single_update(update)
                
                self.stats['updates_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing update: {e}")
                self.stats['errors'] += 1
        
        # Логируем статистику каждые 100 обновлений
        if self.stats['updates_processed'] % 100 == 0:
            logger.info(
                f"Processed {self.stats['updates_processed']} updates, "
                f"offset: {self.offset}"
            )
    
    async def _process_single_update(self, update: Dict[str, Any]):
        """Обработка одного обновления"""
        # Проверяем тип update
        if 'message' in update:
            await self._handle_message(update['message'])
        elif 'callback_query' in update:
            await self._handle_callback_query(update['callback_query'])
        elif 'inline_query' in update:
            await self._handle_inline_query(update['inline_query'])
        # Добавьте другие типы по необходимости

    async def download_voice_file(self, msg:types.Message)->str:
        file_id = msg.voice.file_id
        file = await self.bot.get_file(file_id)
        file_path = file.file_path
        timestamp = msg.date.strftime("%Y%m%d_%H%M%S")
        filename = f"voice_{msg.from_user.id}_{timestamp}.ogg"
        download_path = Path(data_dir,filename)

        await self.bot.download_file(
            file_path=file_path,
            destination=download_path
        )
        return str(download_path)

    async def handle_voice(self, msg:types.Message):
        """Обработчик голосовых сообщений"""
        try:
            user = msg.from_user

            # Скачиваем голосовое сообщение
            filename = await self.download_voice_file(msg)
            filename = export_to_mp3(str(filename))

            logger.info(f"Получено голосовое сообщение от {user.first_name}. Файл сохранен как {filename}")
            publisher.publish_voice(msg, filename)

        except Exception as e:
            logger.error(f"Ошибка при обработке голосового сообщения: {e}")


    async def handle_text(self, msg:types.Message):
        """Обработчик текстовых сообщений"""
        publisher.publish_text(msg)

        await self._send_message(self,msg.chat.id, "Сообщение принято. Спасибо!")




    async def _handle_message(self, message: Dict[str, Any]):
        """Обработка сообщения"""
        try:
            text = message.get('text', '').strip()
            voice = message.get('voice', None)
            chat_id = message['chat']['id']
            message_id = message['message_id']
            msg_obj = await self.make_message_object(chat_id, message, message_id, text, voice)

            # Проверяем команды
            if text.startswith('/'):
                command = text.split()[0].lower()
                
                if command in self.handlers:
                    await self.handlers[command](msg_obj)
            
            # Также можно обрабатывать не-командные сообщения
            else:
                # Простой эхо-ответ
                response = f"Вы сказали: {text}"
                await self._send_message(chat_id, response)
                if text:
                    await self.handle_text(msg_obj)

                if voice:
                    await self.handle_voice(msg_obj)
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def make_message_object(self, chat_id, message, message_id, text, voice):
        msg_obj = types.Message(
            message_id=message_id,
            date=message.get('date', 0),
            chat=types.Chat(
                id=chat_id,
                type=message['chat'].get('type', 'private')
            ),
            from_user=types.User(
                id=message['from']['id'],
                is_bot=message['from'].get('is_bot', False),
                first_name=message['from'].get('first_name', ''),
                last_name=message['from'].get('last_name', ''),
                username=message['from'].get('username', '')
            ) if 'from' in message else None,
            text=text,
            voice=voice
        )
        return msg_obj

    async def _handle_callback_query(self, callback_query: Dict[str, Any]):
        """Обработка callback query"""
        try:
            # Отвечаем на callback query
            await self._answer_callback_query(
                callback_query['id'],
                text="Обработано"
            )
            
            # Можно добавить логику обработки callback данных
            data = callback_query.get('data', '')
            logger.info(f"Callback query received: {data}")
            
        except Exception as e:
            logger.error(f"Error handling callback query: {e}")
    
    async def _handle_inline_query(self, inline_query: Dict[str, Any]):
        """Обработка inline query"""
        try:
            query_id = inline_query['id']
            query = inline_query.get('query', '')
            
            # Ответ на inline query
            await self._answer_inline_query(
                query_id,
                results=[]
            )
            
        except Exception as e:
            logger.error(f"Error handling inline query: {e}")
    
    async def _send_message(self, chat_id: int, text: str, **kwargs):
        """Отправка сообщения"""
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                **kwargs
            )
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
    
    async def _answer_callback_query(self, callback_query_id: str, **kwargs):
        """Ответ на callback query"""
        try:
            await self.bot.answer_callback_query(
                callback_query_id=callback_query_id,
                **kwargs
            )
        except Exception as e:
            logger.error(f"Failed to answer callback query: {e}")
    
    async def _answer_inline_query(self, inline_query_id: str, results: list, **kwargs):
        """Ответ на inline query"""
        try:
            await self.bot.answer_inline_query(
                inline_query_id=inline_query_id,
                results=results,
                **kwargs
            )
        except Exception as e:
            logger.error(f"Failed to answer inline query: {e}")
    
    def _get_bot_status(self) -> Dict[str, Any]:
        """Получение статуса бота"""
        uptime = datetime.now() - self.stats['start_time']
        
        return {
            'instance_id': self.leader_election.instance_id[:8],
            'is_leader': self.leader_election.is_leader,
            'is_running': self.is_running,
            'offset': self.offset,
            'uptime': str(uptime),
            'stats': {
                'updates_received': self.stats['updates_received'],
                'updates_processed': self.stats['updates_processed'],
                'errors': self.stats['errors'],
                'last_update_time': self.stats['last_update_time'].isoformat() 
                    if self.stats['last_update_time'] else None
            },
            'cluster': self.leader_election.get_cluster_status()
        }
    
    async def start(self):
        """Запуск бота"""
        if self.is_running:
            return
        
        self.is_running = True
        logger.info(f"Starting bot instance: {self.leader_election.instance_id}")
        
        try:
            # Запускаем leader election
            await self.leader_election.start()
            
        except KeyboardInterrupt:
            await self.stop()
        except Exception as e:
            logger.error(f"Bot failed to start: {e}")
            await self.stop()
    
    async def stop(self):
        """Остановка бота"""
        self.is_running = False
        
        logger.info("Stopping bot...")
        
        # Останавливаем leader election
        await self.leader_election.stop()
        
        # Останавливаем polling
        if self.polling_task and not self.polling_task.done():
            self.polling_task.cancel()
            try:
                await self.polling_task
            except asyncio.CancelledError:
                pass
        
        # Закрываем сессию
        if self.session and not self.session.closed:
            await self.session.close()
        
        # Закрываем бота
        await self.bot.session.close()
        
        logger.info("Bot stopped")

BOT_TOKEN = "8535593950:AAHGhZ4mRK7LWWl2Q63-c5iC7aKS0E3gWJ4"


async def main():
    bot = DistributedLongPollingBot(token=BOT_TOKEN,
                                    etcd_hosts=["localhost"]
                                   )
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())
