import requests
import logging
import os
from airflow.models import Variable

# --- CONFIGURATION ---
# Try to get from Airflow Variables first, then Env Var, then fallback (or fail)
# In Airflow UI: Admin -> Variables -> Key: DISCORD_WEBHOOK_URL
DISCORD_WEBHOOK_URL = Variable.get("DISCORD_WEBHOOK_URL", default_var=os.getenv("DISCORD_WEBHOOK_URL"))
DISCORD_USER_ID = Variable.get("DISCORD_USER_ID", default_var=os.getenv("DISCORD_USER_ID", "902058850599993404"))

def send_discord_message(payload):
    """Helper function to send request to Discord"""
    if not DISCORD_WEBHOOK_URL or "YOUR_WEBHOOK" in DISCORD_WEBHOOK_URL:
        logging.warning("⚠️ Discord Webhook URL is not set. Skipping alert.")
        return

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        logging.info("✅ Notification sent to Discord.")
    except Exception as e:
        logging.error(f"❌ Failed to send notification: {e}")

def send_failure_alert(context):
    """
    Called when a Task FAILS.
    Tags the user and provides a link to the log.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    log_url = ti.log_url
    exception = context.get('exception')

    payload = {
        "content": f"🚨 **CRITICAL ALERT** <@{DISCORD_USER_ID}>! Warehouse Pipeline Failed!",
        "embeds": [{
            "title": "❌ ETL Task Failed",
            "color": 15548997, # Red
            "fields": [
                {"name": "DAG", "value": f"`{dag_id}`", "inline": True},
                {"name": "Task", "value": f"`{task_id}`", "inline": True},
                {"name": "Error", "value": f"```{str(exception)[:200]}...```", "inline": False}
            ],
            "description": f"[👉 Click here to fix]({log_url})",
            "footer": {"text": "Please investigate immediately."}
        }]
    }
    send_discord_message(payload)

def send_success_alert(context):
    """
    Called when a Task SUCCEEDS.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date')

    payload = {
        "content": "✅ **Status Report:**",
        "embeds": [{
            "title": "Warehouse Task Completed",
            "color": 5763719, # Green
            "description": f"Task **{task_id}** in DAG **{dag_id}** executed successfully.",
            "footer": {"text": f"Exec Date: {execution_date}"}
        }]
    }
    send_discord_message(payload)