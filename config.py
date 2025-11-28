"""
Файл конфигурации для настроек бота и подключения к базе данных.
"""

import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Класс для хранения конфигурации приложения."""
    
    # Настройки Telegram бота
    BOT_TOKEN: str = os.getenv('BOT_TOKEN', '')
    
    # Настройки базы данных
    DB_HOST: str = os.getenv('DB_HOST', 'localhost')
    DB_PORT: int = int(os.getenv('DB_PORT', '5432'))
    DB_NAME: str = os.getenv('DB_NAME', '')
    DB_USER: str = os.getenv('DB_USER', '')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', '')
    
    # Настройки логирования
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    # Уровень логирования для SQL-запросов (по умолчанию берётся из LOG_LEVEL)
    SQL_LOG_LEVEL: str = os.getenv('SQL_LOG_LEVEL', os.getenv('LOG_LEVEL', 'INFO'))
    # Включить ли логирование SQL (булево). Значения true/1/yes включают логирование.
    SQL_LOG_ENABLED: bool = os.getenv('SQL_LOG_ENABLED', 'False').lower() in ('1', 'true', 'yes')
    
    @classmethod
    def validate(cls) -> bool:
        """
        Проверка корректности конфигурации.
        
        Returns:
            bool: True если все обязательные параметры заданы
        """
        required_fields = [
            cls.BOT_TOKEN,
            cls.DB_NAME,
            cls.DB_USER,
            cls.DB_PASSWORD
        ]
        
        return all(field for field in required_fields)
    
    @classmethod
    def get_missing_fields(cls) -> list:
        """
        Получение списка незаполненных обязательных полей.
        
        Returns:
            list: Список имен незаполненных полей
        """
        missing = []
        
        if not cls.BOT_TOKEN:
            missing.append('BOT_TOKEN')
        if not cls.DB_NAME:
            missing.append('DB_NAME')
        if not cls.DB_USER:
            missing.append('DB_USER')
        if not cls.DB_PASSWORD:
            missing.append('DB_PASSWORD')
            
        return missing


# Создание экземпляра конфигурации
config = Config()
