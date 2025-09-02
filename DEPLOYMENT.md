# Инструкция по развертыванию

## Локальный запуск

### 1. Клонирование репозитория
```bash
git clone https://github.com/mayorov156/podpiski.git
cd podpiski
```

### 2. Создание виртуального окружения
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate  # Windows
```

### 3. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 4. Настройка конфигурации
```bash
cp config.py.example config.py
# Отредактируйте config.py и заполните необходимые параметры
```

### 5. Инициализация базы данных
```bash
python init_db_run.py
```

### 6. Запуск бота
```bash
python main.py
```

## Развертывание на сервере

### Способ 1: Systemd сервис

1. Скопируйте проект на сервер
2. Запустите скрипт деплоя:
```bash
./deploy.sh
```

### Способ 2: Docker

1. Установите Docker и Docker Compose
2. Запустите:
```bash
docker-compose up -d
```

### Способ 3: Ручная настройка

1. Создайте пользователя для бота:
```bash
sudo useradd -r -s /bin/false bot
```

2. Скопируйте файлы в `/opt/bot/`

3. Создайте systemd сервис:
```bash
sudo cp bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable bot
sudo systemctl start bot
```

## Мониторинг и логи

### Просмотр статуса
```bash
sudo systemctl status bot
```

### Просмотр логов
```bash
sudo journalctl -u bot -f
```

### Перезапуск бота
```bash
sudo systemctl restart bot
```

## Обновление

### Автоматическое обновление
```bash
./deploy.sh
```

### Ручное обновление
```bash
git pull origin main
sudo systemctl restart bot
```

## Безопасность

1. Не публикуйте `config.py` с реальными токенами
2. Используйте отдельного пользователя для запуска бота
3. Ограничьте доступ к файлам бота
4. Регулярно обновляйте зависимости

## Устранение неполадок

### Бот не запускается
1. Проверьте логи: `sudo journalctl -u bot -f`
2. Убедитесь, что config.py настроен правильно
3. Проверьте права доступа к файлам

### Проблемы с базой данных
1. Проверьте права доступа к файлу БД
2. Убедитесь, что SQLite установлен
3. Попробуйте пересоздать БД: `python init_db_run.py`

### Проблемы с зависимостями
1. Обновите pip: `pip install --upgrade pip`
2. Переустановите зависимости: `pip install -r requirements.txt --force-reinstall`
