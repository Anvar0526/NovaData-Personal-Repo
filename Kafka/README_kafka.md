# Миграция данных PostgreSQL → Kafka → ClickHouse

## Описание
Пайплайн для безопасной миграции событий пользователей из PostgreSQL в ClickHouse через Kafka с защитой от дубликатов с помощью флага `sent_to_kafka` в PostgreSQL.

## Как пользоваться
1. Инфраструктура поднимается с помощью команды `docker-compose up -d`
2. Запускаем продюсер с пощощью команды `python producer.py` в терминале
3. Запускаем консьюмер с помощью команды `python consumer.py` в терминале

## Описание
**Продюсер** (`producer.py`): читает из PostgreSQL записи с `sent_to_kafka = FALSE`, отправляет их в Kafka топик `user_events`, после отправки ставит `sent_to_kafka = TRUE`.

**Консюмер** (`consumer.py`): читает топик `user_events` в Kafka, сохраняет данные в ClickHouse таблицу через движок `MergeTree`.

## Проверка
После запуска проверьте данные в ClickHouse: `SELECT * FROM user_logins`