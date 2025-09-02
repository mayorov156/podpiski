#!/bin/bash

# Скрипт для деплоя бота на сервер

echo "🚀 Начинаем деплой Telegram бота..."

# Проверяем, что мы в правильной директории
if [ ! -f "main.py" ]; then
    echo "❌ Ошибка: main.py не найден. Убедитесь, что вы находитесь в корневой папке проекта."
    exit 1
fi

# Останавливаем бота если он запущен
echo "🛑 Останавливаем бота..."
sudo systemctl stop bot.service 2>/dev/null || true

# Обновляем код из Git
echo "📥 Обновляем код из Git..."
git pull origin main

# Активируем виртуальное окружение и обновляем зависимости
echo "📦 Обновляем зависимости..."
source venv/bin/activate
pip install -r requirements.txt

# Проверяем конфигурацию
if [ ! -f "config.py" ]; then
    echo "⚠️  config.py не найден. Копируем пример..."
    cp config.py.example config.py
    echo "📝 Пожалуйста, отредактируйте config.py и заполните необходимые параметры"
    echo "   Затем запустите деплой снова"
    exit 1
fi

# Инициализируем базу данных
echo "🗄️  Инициализируем базу данных..."
python init_db_run.py

# Копируем systemd сервис
echo "⚙️  Настраиваем systemd сервис..."
sudo cp bot.service /etc/systemd/system/
sudo systemctl daemon-reload

# Запускаем бота
echo "▶️  Запускаем бота..."
sudo systemctl enable bot.service
sudo systemctl start bot.service

# Проверяем статус
echo "📊 Проверяем статус бота..."
sleep 3
sudo systemctl status bot.service --no-pager

echo "✅ Деплой завершен!"
echo "📋 Полезные команды:"
echo "   sudo systemctl status bot.service  - проверить статус"
echo "   sudo systemctl stop bot.service    - остановить бота"
echo "   sudo systemctl start bot.service   - запустить бота"
echo "   sudo journalctl -u bot.service -f  - просмотр логов"
