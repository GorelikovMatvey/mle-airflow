# mle-airflow

Учебные DAG для подготовки данных об оттоке клиентов. `prepare_churn_dataset` собирает таблицу `users_churn`, `clean_churn_dataset` очищает её и записывает результат в `clean_users_churn`. `alt_churn` — вариант первого DAG через PythonOperator с уведомлениями в Telegram.

Настройки БД указываются в `.env` по образцу `.env_template`. Для DAG нужны подключения Airflow `source_db` и `destination_db`, для уведомлений — `telegram_notifications`.

Запуск Airflow на учебной ВМ: `docker compose up -d --build`.
