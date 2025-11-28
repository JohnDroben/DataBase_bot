#!/usr/bin/env python3
"""
Быстрая проверка состояния бота
Run: python check_status.py
"""

from config import config
from database import init_database

print("\n" + "="*60)
print("TELEGRAM FINANCE BOT - STATUS CHECK")
print("="*60)

# Configuration
print("\n[CONFIG]")
print(f"  BOT_TOKEN: {'Loaded' if config.BOT_TOKEN else 'MISSING'}")
print(f"  DB_HOST:   {config.DB_HOST}")
print(f"  DB_NAME:   {config.DB_NAME}")
print(f"  Status:    {'VALID' if config.validate() else 'INVALID - ' + str(config.get_missing_fields())}")

# Database
print("\n[DATABASE]")
db = init_database(
    config.DB_HOST,
    config.DB_PORT,
    config.DB_NAME,
    config.DB_USER,
    config.DB_PASSWORD
)

if db.connect():
    print(f"  Connection: OK")
    tables = db.get_all_tables()
    print(f"  Tables:     {len(tables)} found")
    if tables:
        for t in tables:
            print(f"    - {t}")
    else:
        print("    (Database is empty - needs initialization)")
    db.disconnect()
else:
    print(f"  Connection: FAILED")

# Summary
print("\n[READY TO RUN]")
print("  Command: python bot.py")
print("="*60 + "\n")
