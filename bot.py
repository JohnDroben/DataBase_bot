"""
Основной файл Telegram бота для контроля доходов/расходов.
Содержит хендлеры и логику работы финансового бота.
"""

import logging
import telebot
from telebot import types
from database import init_database, get_db_manager
from excel import backup_user, backup_transaction
from config import config
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = telebot.TeleBot(config.BOT_TOKEN)

# Глобальный менеджер БД (останется открытым во время работы бота)
global_db_manager = None
# Глобальная ссылка на экземпляр обработчиков для форвардинга callback'ов
finance_handlers = None


def _callback_forwarder(call):
    """Module-level forwarder for callback queries to the active handlers instance."""
    try:
        logger.debug("Incoming callback query: %s", getattr(call, 'data', None))
    except Exception:
        pass
    if finance_handlers is None:
        logger.warning("No finance_handlers instance to handle callback")
        return
    try:
        finance_handlers.handle_callback(call)
    except Exception:
        logger.exception("Error while handling callback")


# Register module-level forwarder so callbacks always reach the current handlers
bot.callback_query_handler(func=lambda call: True)(_callback_forwarder)


class FinanceBotHandlers:
    """Класс для хранения всех хендлеров финансового бота."""
    
    def __init__(self):
        self.db_manager = None
        self.user_states = {}  # Для отслеживания состояния пользователей
        self.setup_handlers()
    
    def setup_handlers(self):
        """Настройка всех хендлеров бота."""
        # Команды
        bot.message_handler(commands=['start'])(self.start_command)
        bot.message_handler(commands=['help'])(self.help_command)
        bot.message_handler(commands=['status'])(self.status_command)
        bot.message_handler(commands=['balance'])(self.balance_command)
        bot.message_handler(commands=['report'])(self.report_command)
        bot.message_handler(commands=['budget'])(self.budget_command)
        bot.message_handler(commands=['categories'])(self.categories_command)
        bot.message_handler(commands=['add'])(self.add_command)
        bot.message_handler(commands=['cancel'])(self.cancel_command)
        
        # Обработчики сообщений
        bot.message_handler(func=lambda message: True)(self.handle_message)
        
        # Обработчики callback-запросов
        bot.callback_query_handler(func=lambda call: True)(self.handle_callback)

    def clear_user_state(self, user_id):
        """Очищает состояние пользователя."""
        if user_id in self.user_states:
            del self.user_states[user_id]

    def start_command(self, message):
        """Обработчик команды /start."""
        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.first_name or "пользователь"
        
        logger.info(f"Пользователь {username} (ID: {user_id}) запустил бота")
        self.clear_user_state(user_id)
        
        # Регистрируем пользователя в базе данных
        if self.db_manager:
            if self.db_manager.add_user(user_id, username):
                # Сохраняем в Excel
                backup_user(user_id, username, 'RUB')
        
        welcome_text = (
            f"Привет, {username}! 👋\n\n"
            "💰 Я бот для контроля ваших доходов и расходов.\n\n"
            "Используйте команду /add, чтобы добавить новую транзакцию.\n\n"
            "📊 /report - для анализа трат\n"
            "💳 /budget - для управления лимитами\n"
            "📋 /help - для полного списка команд"
        )
        
        bot.send_message(message.chat.id, welcome_text)
    
    def help_command(self, message):
        """Обработчик команды /help."""
        self.clear_user_state(message.from_user.id)
        help_text = (
            "💰 Финансовый бот - Справка\n\n"
            "📝 Добавление операций:\n"
            "/add - начать добавление новой транзакции (дохода или расхода).\n\n"
            "📊 Команды:\n"
            "/balance - Текущий баланс\n"
            "/report неделя - Отчет за неделю\n"
            "/status - Статус аккаунта\n"
            "/categories - Категории транзакций\n"
            "/budget - Управление лимитами\n"
            "/cancel - Отменить операцию\n"
        )
        bot.send_message(message.chat.id, help_text)
    
    def status_command(self, message):
        """Обработчик команды /status."""
        self.clear_user_state(message.from_user.id)
        user_id = message.from_user.id
        
        if not self.db_manager:
            bot.send_message(message.chat.id, "Ошибка подключения к базе данных")
            return
        
        user = self.db_manager.get_user(user_id)
        if user:
            text = f"Статус аккаунта:\nID: {user.get('user_id')}\nИмя: {user.get('name')}\nВалюта: {user.get('currency', 'RUB')}"
        else:
            text = "Пользователь не найден в базе данных"
        
        bot.send_message(message.chat.id, text)
    
    def balance_command(self, message):
        """Обработчик команды /balance."""
        self.clear_user_state(message.from_user.id)
        user_id = message.from_user.id
        
        if not self.db_manager:
            bot.send_message(message.chat.id, "Ошибка подключения к базе данных")
            return
        
        transactions = self.db_manager.get_transactions(user_id, limit=100)
        
        income = sum(t['amount'] for t in transactions if t['type'] == 'income')
        expense = sum(t['amount'] for t in transactions if t['type'] == 'expense')
        balance = income - expense
        
        text = f"Financial Summary:\nIncome: {income}\nExpense: {expense}\nBalance: {balance}"
        bot.send_message(message.chat.id, text)
    
    def report_command(self, message):
        """Обработчик команды/report."""
        self.clear_user_state(message.from_user.id)
        user_id = message.from_user.id
        
        if not self.db_manager:
            bot.send_message(message.chat.id, "Ошибка подключения к базе данных")
            return
        
        now = datetime.now()
        summary = self.db_manager.get_monthly_summary(user_id, now.year, now.month)
        
        text = f"Месячный отчет:\nДоход: {summary['income']}\nРасход: {summary['expense']}\nБаланс: {summary['balance']}"
        bot.send_message(message.chat.id, text)
    
    def budget_command(self, message):
        """Обработчик команды /budget."""
        self.clear_user_state(message.from_user.id)
        user_id = message.from_user.id
        
        if not self.db_manager:
            bot.send_message(message.chat.id, "Ошибка подключения к базе данных")
            return
        
        budgets = self.db_manager.get_budgets(user_id)
        
        if not budgets:
            text = "Лимиты не установлены"
        else:
            text = "Ваши лимиты:\n"
            for b in budgets:
                text += f"- {b['category']}: {b['limit_amount']} ({b['period']})\n"
        
        bot.send_message(message.chat.id, text)
    
    def categories_command(self, message):
        """Обработчик команды /categories."""
        self.clear_user_state(message.from_user.id)
        user_id = message.from_user.id
        
        if not self.db_manager:
            bot.send_message(message.chat.id, "Ошибка подключения к базе данных")
            return
        
        categories = self.db_manager.get_categories(user_id)
        
        if not categories:
            text = "Категории не найдены"
        else:
            text = "Ваши категории:\n"
            for c in categories:
                text += f"- {c['name']} ({c['type']})\n"
        
        bot.send_message(message.chat.id, text)
    
    def add_command(self, message):
        """Obrabotchik komandy /add."""
        user_id = message.from_user.id
        self.user_states[user_id] = {'state': 'waiting_type'}
        
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("Доход", callback_data="type_income"),
            types.InlineKeyboardButton("Расход", callback_data="type_expense")
        )
        bot.send_message(message.chat.id, "Выберите тип транзакции:", reply_markup=markup)
    
    def cancel_command(self, message):
        """Обработчик команды /cancel."""
        user_id = message.from_user.id
        self.clear_user_state(user_id)
        bot.send_message(message.chat.id, "Операция отменена")
    
    def handle_message(self, message):
        """Обработчик общих сообщений."""
        user_id = message.from_user.id
        
        if user_id not in self.user_states:
            bot.send_message(message.chat.id, "Используйте /help для просмотра доступных команд")
            return
        
        state_info = self.user_states[user_id]
        
        if state_info.get('state') == 'waiting_amount':
            try:
                amount = float(message.text)
                state_info['amount'] = amount
                state_info['state'] = 'waiting_category'
                
                categories = self.db_manager.get_categories(user_id, state_info.get('type'))
                if categories:
                    markup = types.InlineKeyboardMarkup()
                    for cat in categories[:5]:
                        markup.add(types.InlineKeyboardButton(cat['name'], callback_data=f"cat_{cat['id']}"))
                    bot.send_message(message.chat.id, "Выберите категорию:", reply_markup=markup)
                else:
                    bot.send_message(message.chat.id, "Категории недоступны")
            except ValueError:
                bot.send_message(message.chat.id, "Неверная сумма. Введите число:")
        
        elif state_info.get('state') == 'waiting_description':
            state_info['description'] = message.text
            
            if self.db_manager:
                success = self.db_manager.add_transaction(
                    user_id,
                    state_info['type'],
                    state_info['amount'],
                    state_info.get('category', 'Other'),
                    state_info['description']
                )
                if success:
                    backup_transaction(
                        user_id,
                        state_info['type'],
                        state_info['amount'],
                        state_info.get('category', 'Other'),
                        state_info['description']
                    )
                    bot.send_message(message.chat.id, "Транзакция успешно сохранена")
                else:
                    bot.send_message(message.chat.id, "Ошибка при сохранении транзакции. Администратор получит лог.")
                    logger.error(f"Не удалось сохранить транзакцию для пользователя {user_id}: {state_info}")
            else:
                bot.send_message(message.chat.id, "Ошибка подключения к базе данных")
            
            self.clear_user_state(user_id)
    
    def handle_callback(self, call):
        """Обработчик callback запросов."""
        user_id = call.from_user.id
        
        if call.data.startswith("type_"):
            transaction_type = call.data.split("_")[1]
            self.user_states[user_id] = {'state': 'waiting_amount', 'type': transaction_type}
            bot.send_message(call.message.chat.id, "Введите сумму:")
        
        elif call.data.startswith("cat_"):
            category_id = call.data.split("_")[1]
            if user_id in self.user_states:
                self.user_states[user_id]['category'] = category_id
                self.user_states[user_id]['state'] = 'waiting_description'
                bot.send_message(call.message.chat.id, "Введите описание (или /cancel):")
        
        bot.answer_callback_query(call.id)


def main():
    """Main entry point for the bot."""
    global global_db_manager
    global finance_handlers
    
    # Validate configuration
    if not config.validate():
        missing = config.get_missing_fields()
        logger.error(f"Ошибка проверки конфигурации. Отсутствуют поля: {missing}")
        print(f"ОШИБКА: Отсутствуют поля конфигурации: {missing}")
        print("Пожалуйста, создайте файл .env с необходимыми настройками. См. env.example")
        return
    
    # Initialize database
    logger.info("Инициализация подключения к базе данных...")
    global_db_manager = init_database(
        config.DB_HOST,
        config.DB_PORT,
        config.DB_NAME,
        config.DB_USER,
        config.DB_PASSWORD
    )
    
    # Try to connect to database
    if not global_db_manager.connect():
        logger.error("Не удалось подключиться к базе данных")
        print("ОШИБКА: Не удалось подключиться к базе данных. Проверьте учетные данные БД в файле .env")
        return
    
    logger.info("Подключение к базе данных успешно")
    
    # Initialize bot handlers
    handlers = FinanceBotHandlers()
    handlers.db_manager = global_db_manager
    # Make handlers available to module-level forwarder
    finance_handlers = handlers

    # Update listener to log raw updates from Telegram (helps debug missing callbacks)
    try:
        def _log_updates(updates):
            for u in updates:
                logger.debug("Raw update: %s", u)

        bot.set_update_listener(_log_updates)
        logger.debug("Update listener registered to log raw updates")
    except Exception:
        logger.exception("Failed to register update listener")
    
    # Check database tables
    tables = global_db_manager.get_all_tables()
    logger.info(f"Таблицы базы данных: {tables}")
    if not tables:
        logger.info("Таблицы не найдены — создаём схему базы данных...")
        if global_db_manager.create_tables():
            logger.info("Схема базы данных успешно создана")
            tables = global_db_manager.get_all_tables()
            logger.info(f"Таблицы после создания: {tables}")
        else:
            logger.error("Не удалось создать схему базы данных")
    
    # Start polling
    logger.info("Запуск опроса бота...")
    print("Бот запущен... Нажмите Ctrl+C для остановки")
    
    try:
        bot.infinity_polling()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
        print("Бот остановлен")
    finally:
        if global_db_manager:
            global_db_manager.disconnect()
            logger.info("Соединение с базой данных закрыто")

if __name__ == '__main__':
    main()

        

