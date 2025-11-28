"""
Модуль для работы с базой данных финансового бота.
Обеспечивает подключение к удаленной БД PostgreSQL и выполнение SQL-запросов
для управления доходами/расходами.
"""

import pg8000.dbapi as pg8000
from pg8000.dbapi import OperationalError, DatabaseError
from typing import List, Dict, Any, Optional, Tuple
import logging
from config import config
from datetime import datetime, timedelta
import calendar

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prepare numeric level for SQL logging
try:
    SQL_LOG_LEVEL = getattr(logging, config.SQL_LOG_LEVEL.upper(), logging.DEBUG)
except Exception:
    SQL_LOG_LEVEL = logging.DEBUG

def _log_sql(query: str, params: Optional[tuple]):
    """Helper to log SQL queries and params at configured level."""
    # Respect global enable flag from config
    if not getattr(config, 'SQL_LOG_ENABLED', False):
        return
    if not query:
        return
    # Shorten query for logs but keep full params for debugging
    q = " ".join(query.strip().split())
    if params is None:
        logger.log(SQL_LOG_LEVEL, "SQL: %s", q)
    else:
        logger.log(SQL_LOG_LEVEL, "SQL: %s | params: %r", q, params)


class DatabaseManager:
    """Класс для управления подключением к базе данных и выполнения запросов."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """
        Инициализация менеджера базы данных.
        
        Args:
            host: Хост базы данных
            port: Порт базы данных
            database: Имя базы данных
            user: Имя пользователя
            password: Пароль
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
    
    def connect(self) -> bool:
        """
        Установка подключения к базе данных.
        
        Returns:
            bool: True если подключение успешно, False в противном случае
        """
        try:
            self.connection = pg8000.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            logger.info(f"Успешное подключение к базе данных {self.database}")
            return True
        except OperationalError as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            return False
    
    def disconnect(self):
        """Закрытие подключения к базе данных."""
        if self.connection and getattr(self.connection, 'closed', 1) == 0:
            self.connection.close()
            logger.info("Подключение к базе данных закрыто")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Выполнение SELECT запроса.
        
        Args:
            query: SQL запрос
            params: Параметры для запроса
            
        Returns:
            List[Dict]: Список словарей с результатами запроса
        """
        # Попытка переподключиться если соединение потеряно
        if not self.connection or not self.check_connection():
            logger.warning("Соединение потеряно, попытка переподключения...")
            if not self.connect():
                logger.error("Нет подключения к базе данных")
                return []
        
        try:
            _log_sql(query, params)
            cursor = self.connection.cursor()
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            # Normalize column names to lower-case to provide consistent keys
            columns = [desc[0].lower() for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return results
            
        except DatabaseError as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            return []
    
    def execute_insert(self, query: str, params: Optional[tuple] = None) -> bool:
        """
        Выполнение INSERT запроса.
        
        Args:
            query: SQL запрос
            params: Параметры для запроса
            
        Returns:
            bool: True если запрос выполнен успешно
        """
        # Попытка переподключиться если соединение потеряно
        if not self.connection or not self.check_connection():
            logger.warning("Соединение потеряно, попытка переподключения...")
            if not self.connect():
                logger.error("Нет подключения к базе данных")
                return False
        
        try:
            _log_sql(query, params)
            cursor = self.connection.cursor()
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            self.connection.commit()
            cursor.close()
            return True
            
        except DatabaseError as e:
            logger.error(f"Ошибка выполнения INSERT запроса: {e}")
            self.connection.rollback()
            return False
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> bool:
        """
        Выполнение UPDATE запроса.
        
        Args:
            query: SQL запрос
            params: Параметры для запроса
            
        Returns:
            bool: True если запрос выполнен успешно
        """
        # Попытка переподключиться если соединение потеряно
        if not self.connection or not self.check_connection():
            logger.warning("Соединение потеряно, попытка переподключения...")
            if not self.connect():
                logger.error("Нет подключения к базе данных")
                return False
        
        try:
            _log_sql(query, params)
            cursor = self.connection.cursor()
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            affected = getattr(cursor, 'rowcount', None)
            self.connection.commit()
            cursor.close()
            # Return True only if rows were affected
            return bool(affected and affected > 0)
            
        except DatabaseError as e:
            logger.error(f"Ошибка выполнения UPDATE запроса: {e}")
            try:
                self.connection.rollback()
            except Exception:
                pass
            return False
    
    def get_table_structure(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Получение структуры таблицы из базы данных.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            List[Dict]: Информация о колонках таблицы
        """
        query = """
        SELECT 
            column_name AS COLUMN_NAME,
            data_type AS DATA_TYPE,
            is_nullable AS IS_NULLABLE,
            column_default AS COLUMN_DEFAULT
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = %s
        ORDER BY ordinal_position
        """
        
        return self.execute_query(query, (table_name,))
    
    def get_all_tables(self) -> List[str]:
        """
        Получение списка всех таблиц в базе данных.
        
        Returns:
            List[str]: Список имен таблиц
        """
        query = """
        SELECT table_name AS TABLE_NAME
        FROM information_schema.tables
        WHERE table_schema = current_schema()
        """
        
        results = self.execute_query(query)
        # execute_query нормализует имена колонок в нижний регистр
        return [row.get('table_name') for row in results]
    
    def check_connection(self) -> bool:
        """
        Проверка состояния подключения к базе данных.
        
        Returns:
            bool: True если подключение активно
        """
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception as e:
            logger.debug(f"Соединение неактивно: {e}")
            return False

    def create_tables(self) -> bool:
        """
        Создает необходимые таблицы, если они отсутствуют.

        Returns:
            bool: True если создание прошло успешно или таблицы уже существуют
        """
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                name TEXT,
                currency TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                type TEXT,
                amount NUMERIC,
                category TEXT,
                description TEXT,
                date TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name TEXT,
                type TEXT,
                user_id BIGINT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS budgets (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                category TEXT,
                limit_amount NUMERIC,
                period TEXT
            )
            """
        ]

        try:
            cursor = self.connection.cursor()
            for ddl in ddl_statements:
                cursor.execute(ddl)
            self.connection.commit()
            cursor.close()
            logger.info("Схема базы данных создана/проверена")
            return True
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц: {e}")
            try:
                self.connection.rollback()
            except Exception:
                pass
            return False
    
    # === МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ===
    
    def add_user(self, user_id: int, name: str, currency: str = 'RUB') -> bool:
        """
        Добавление нового пользователя.
        
        Args:
            user_id: ID пользователя в Telegram
            name: Имя пользователя
            currency: Валюта пользователя
            
        Returns:
            bool: True если пользователь добавлен успешно
        """
        # Пытаемся обновить; если обновление не затронуло строк — вставляем
        update_q = "UPDATE users SET name = %s, currency = %s WHERE user_id = %s"
        updated = self.execute_update(update_q, (name, currency, user_id))
        if updated:
            logger.info(f"User {user_id} updated")
            return True

        insert_q = "INSERT INTO users (user_id, name, currency) VALUES (%s, %s, %s)"
        inserted = self.execute_insert(insert_q, (user_id, name, currency))
        if inserted:
            logger.info(f"User {user_id} inserted")
        else:
            logger.error(f"Failed to insert user {user_id}")
        return inserted
    
    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение информации о пользователе.
        
        Args:
            user_id: ID пользователя в Telegram
            
        Returns:
            Dict: Информация о пользователе или None
        """
        query = "SELECT * FROM users WHERE user_id = %s"
        results = self.execute_query(query, (user_id,))
        return results[0] if results else None
    
    # === МЕТОДЫ ДЛЯ РАБОТЫ С ТРАНЗАКЦИЯМИ ===
    
    def add_transaction(self, user_id: int, transaction_type: str, amount: float, 
                       category: str, description: str = "") -> bool:
        """
        Добавление новой транзакции.
        
        Args:
            user_id: ID пользователя
            transaction_type: Тип транзакции ('income' или 'expense')
            amount: Сумма транзакции
            category: Категория транзакции
            description: Описание транзакции
            
        Returns:
            bool: True если транзакция добавлена успешно
        """
        query = """
        INSERT INTO transactions (user_id, type, amount, category, description, date) 
        VALUES (%s, %s, %s, %s, %s, NOW())
        """
        return self.execute_insert(query, (user_id, transaction_type, amount, category, description))
    
    def get_transactions(self, user_id: int, start_date: Optional[datetime] = None, 
                        end_date: Optional[datetime] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получение транзакций пользователя за период.
        
        Args:
            user_id: ID пользователя
            start_date: Начальная дата (по умолчанию - месяц назад)
            end_date: Конечная дата (по умолчанию - сейчас)
            limit: Максимальное количество записей
            
        Returns:
            List[Dict]: Список транзакций
        """
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()
        
        query = """
        SELECT * FROM transactions 
        WHERE user_id = %s AND date BETWEEN %s AND %s 
        ORDER BY date DESC 
        LIMIT %s
        """
        return self.execute_query(query, (user_id, start_date, end_date, limit))
    
    def get_transactions_by_category(self, user_id: int, category: str, 
                                   start_date: Optional[datetime] = None,
                                   end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Получение транзакций по категории.
        
        Args:
            user_id: ID пользователя
            category: Название категории
            start_date: Начальная дата
            end_date: Конечная дата
            
        Returns:
            List[Dict]: Список транзакций по категории
        """
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()
        
        query = """
        SELECT * FROM transactions 
        WHERE user_id = %s AND category = %s AND date BETWEEN %s AND %s 
        ORDER BY date DESC
        """
        return self.execute_query(query, (user_id, category, start_date, end_date))
    
    # === МЕТОДЫ ДЛЯ РАБОТЫ С КАТЕГОРИЯМИ ===
    
    def get_categories(self, user_id: int, category_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получение категорий пользователя.
        
        Args:
            user_id: ID пользователя
            category_type: Тип категории ('income' или 'expense')
            
        Returns:
            List[Dict]: Список категорий
        """
        if category_type:
            query = "SELECT * FROM categories WHERE user_id = %s AND type = %s ORDER BY name"
            return self.execute_query(query, (user_id, category_type))
        else:
            query = "SELECT * FROM categories WHERE user_id = %s ORDER BY name"
            return self.execute_query(query, (user_id,))
    
    def add_category(self, user_id: int, name: str, category_type: str) -> bool:
        """
        Добавление новой категории.
        
        Args:
            user_id: ID пользователя
            name: Название категории
            category_type: Тип категории ('income' или 'expense')
            
        Returns:
            bool: True если категория добавлена успешно
        """
        query = "INSERT INTO categories (name, type, user_id) VALUES (%s, %s, %s)"
        return self.execute_insert(query, (name, category_type, user_id))
    
    def get_default_categories(self) -> List[Dict[str, Any]]:
        """
        Получение стандартных категорий (где user_id = NULL).
        
        Returns:
            List[Dict]: Список стандартных категорий
        """
        query = "SELECT * FROM categories WHERE user_id IS NULL ORDER BY type, name"
        return self.execute_query(query)
    
    # === МЕТОДЫ ДЛЯ РАБОТЫ С БЮДЖЕТАМИ ===
    
    def set_budget(self, user_id: int, category: str, limit_amount: float, period: str) -> bool:
        """
        Установка лимита бюджета для категории.
        
        Args:
            user_id: ID пользователя
            category: Название категории
            limit_amount: Лимит суммы
            period: Период ('month' или 'week')
            
        Returns:
            bool: True если бюджет установлен успешно
        """
        # Двухшаговый upsert: сначала update, затем при отсутствии — insert
        update_q = "UPDATE budgets SET limit_amount = %s, period = %s WHERE user_id = %s AND category = %s"
        if self.execute_update(update_q, (limit_amount, period, user_id, category)):
            return True
        insert_q = "INSERT INTO budgets (user_id, category, limit_amount, period) VALUES (%s, %s, %s, %s)"
        return self.execute_insert(insert_q, (user_id, category, limit_amount, period))
    
    def get_budgets(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Получение всех бюджетов пользователя.
        
        Args:
            user_id: ID пользователя
            
        Returns:
            List[Dict]: Список бюджетов
        """
        query = "SELECT * FROM budgets WHERE user_id = %s ORDER BY category"
        return self.execute_query(query, (user_id,))
    
    def check_budget_exceeded(self, user_id: int, category: str, period: str) -> Tuple[bool, float, float]:
        """
        Проверка превышения бюджета по категории.
        
        Args:
            user_id: ID пользователя
            category: Название категории
            period: Период ('month' или 'week')
            
        Returns:
            Tuple[bool, float, float]: (превышен ли лимит, потрачено, лимит)
        """
        # Получаем лимит бюджета
        budget_query = "SELECT limit_amount FROM budgets WHERE user_id = %s AND category = %s AND period = %s"
        budget_result = self.execute_query(budget_query, (user_id, category, period))
        
        if not budget_result:
            return False, 0, 0
        
        limit_amount = budget_result[0]['limit_amount']
        
        # Определяем период для подсчета трат
        now = datetime.now()
        if period == 'month':
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:  # week
            start_date = now - timedelta(days=now.weekday())
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Подсчитываем потраченную сумму
        spent_query = """
        SELECT COALESCE(SUM(amount), 0) as total 
        FROM transactions 
        WHERE user_id = %s AND category = %s AND type = 'expense' AND date >= %s
        """
        spent_result = self.execute_query(spent_query, (user_id, category, start_date))
        spent_amount = spent_result[0]['total'] if spent_result else 0
        
        return spent_amount > limit_amount, spent_amount, limit_amount
    
    # === МЕТОДЫ ДЛЯ АНАЛИТИКИ И ОТЧЕТОВ ===
    
    def get_monthly_summary(self, user_id: int, year: int, month: int) -> Dict[str, Any]:
        """
        Получение месячной сводки по доходам и расходам.
        
        Args:
            user_id: ID пользователя
            year: Год
            month: Месяц
            
        Returns:
            Dict: Сводка по месяцам
        """
        start_date = datetime(year, month, 1)
        end_date = datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59)
        
        query = """
        SELECT 
            type,
            SUM(amount) as total,
            COUNT(*) as count
        FROM transactions 
        WHERE user_id = %s AND date BETWEEN %s AND %s 
        GROUP BY type
        """
        
        results = self.execute_query(query, (user_id, start_date, end_date))
        
        summary = {
            'income': 0,
            'expense': 0,
            'income_count': 0,
            'expense_count': 0,
            'balance': 0
        }
        
        for row in results:
            if row['type'] == 'income':
                summary['income'] = float(row['total'])
                summary['income_count'] = row['count']
            elif row['type'] == 'expense':
                summary['expense'] = float(row['total'])
                summary['expense_count'] = row['count']
        
        summary['balance'] = summary['income'] - summary['expense']
        return summary
    
    def get_category_summary(self, user_id: int, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        Получение сводки по категориям за период.
        
        Args:
            user_id: ID пользователя
            start_date: Начальная дата
            end_date: Конечная дата
            
        Returns:
            List[Dict]: Сводка по категориям
        """
        query = """
        SELECT 
            category,
            type,
            SUM(amount) as total,
            COUNT(*) as count
        FROM transactions 
        WHERE user_id = %s AND date BETWEEN %s AND %s 
        GROUP BY category, type
        ORDER BY total DESC
        """
        
        return self.execute_query(query, (user_id, start_date, end_date))
    
    def get_weekly_summary(self, user_id: int, start_date: datetime) -> Dict[str, Any]:
        """
        Получение недельной сводки.
        
        Args:
            user_id: ID пользователя
            start_date: Начало недели
            
        Returns:
            Dict: Недельная сводка
        """
        end_date = start_date + timedelta(days=6, hours=23, minutes=59, seconds=59)
        return self.get_monthly_summary(user_id, start_date.year, start_date.month)


# Глобальный экземпляр менеджера базы данных
db_manager = None


def init_database(host: str, port: int, database: str, user: str, password: str) -> DatabaseManager:
    """
    Инициализация глобального менеджера базы данных.
    
    Args:
        host: Хост базы данных
        port: Порт базы данных
        database: Имя базы данных
        user: Имя пользователя
        password: Пароль
        
    Returns:
        DatabaseManager: Экземпляр менеджера базы данных
    """
    global db_manager
    db_manager = DatabaseManager(host, port, database, user, password)
    return db_manager


def get_db_manager() -> Optional[DatabaseManager]:
    """
    Получение глобального менеджера базы данных.
    
    Returns:
        DatabaseManager: Экземпляр менеджера базы данных или None
    """
    return db_manager
