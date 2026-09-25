"""Notifications for the churn DAG."""

from html import escape

from airflow.providers.telegram.hooks.telegram import TelegramHook


def _send_message(text: str) -> None:
    hook = TelegramHook(telegram_conn_id="telegram_notifications")
    hook.send_message({"chat_id": hook.chat_id, "text": text})


def send_telegram_success_message(context) -> None:
    dag_id = escape(context["dag"].dag_id)
    run_id = escape(context["run_id"])
    _send_message(f"DAG {dag_id}, запуск {run_id}: выполнен успешно.")


def send_telegram_failure_message(context) -> None:
    run_id = escape(context["run_id"])
    task_key = escape(context["task_instance_key_str"])
    _send_message(f"Запуск {run_id} завершился ошибкой в задаче {task_key}.")
