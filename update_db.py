import sqlite3
import os

# Путь к файлу базы данных
DB_PATH = "nest_capital_new.db"

# Проверяем, существует ли файл базы данных
if not os.path.exists(DB_PATH):
    print(f"Ошибка: файл базы данных {DB_PATH} не найден.")
    exit(1)

# Подключаемся к базе данных
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Проверяем, есть ли столбцы phone и bank в таблице withdrawal_requests
cursor.execute("PRAGMA table_info(withdrawal_requests)")
columns = cursor.fetchall()
column_names = [column[1] for column in columns]

# Добавляем столбцы, если их нет
if "phone" not in column_names:
    print("Добавление столбца 'phone' в таблицу withdrawal_requests...")
    cursor.execute("ALTER TABLE withdrawal_requests ADD COLUMN phone TEXT")

if "bank" not in column_names:
    print("Добавление столбца 'bank' в таблицу withdrawal_requests...")
    cursor.execute("ALTER TABLE withdrawal_requests ADD COLUMN bank TEXT")

# Сохраняем изменения
conn.commit()
conn.close()

print("База данных успешно обновлена!") 