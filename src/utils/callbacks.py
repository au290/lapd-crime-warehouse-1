import requests
import logging
import os
import csv
from datetime import datetime
from airflow.models import Variable

# --- CONFIGURATION ---
DISCORD_WEBHOOK_URL = Variable.get("DISCORD_WEBHOOK_URL", default_var=os.getenv("DISCORD_WEBHOOK_URL"))
DISCORD_USER_ID = Variable.get("DISCORD_USER_ID", default_var=os.getenv("DISCORD_USER_ID", "902058850599993404"))

# Lokasi File Log (Pastikan folder ini di-mount di docker-compose)
AUDIT_LOG_PATH = "/opt/airflow/logs/pipeline_audit_log.csv"
ERROR_LOG_PATH = "/opt/airflow/logs/pipeline_error_log.csv"

def _write_csv(filepath, row_data, header):
    """
    Fungsi internal untuk menulis ke CSV secara append.
    """
    try:
        # Buat folder jika belum ada
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        file_exists = os.path.isfile(filepath)
        
        with open(filepath, mode='a', newline='') as f:
            writer = csv.writer(f)
            # Tulis Header jika file baru dibuat
            if not file_exists:
                writer.writerow(header)
            
            # Tulis Data
            writer.writerow(row_data)
            
    except Exception as e:
        logging.error(f"❌ Gagal menulis log ke {filepath}: {e}")

def write_audit_log(context, status):
    """
    Mencatat SEMUA status run (Success/Failed) ke Audit Log.
    """
    ti = context.get('task_instance')
    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ti.dag_id,
        ti.task_id,
        status,
        str(context.get('execution_date')),
        str(context.get('exception')) if status == "FAILED" else ""
    ]
    header = ['timestamp', 'dag_id', 'task_id', 'status', 'execution_date', 'error_message']
    
    _write_csv(AUDIT_LOG_PATH, row, header)
    logging.info(f"📝 Audit Log saved.")

def write_error_log(context):
    """
    Mencatat HANYA yang GAGAL ke Error Log khusus.
    """
    ti = context.get('task_instance')
    exception = context.get('exception')
    
    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ti.dag_id,
        ti.task_id,
        ti.log_url, # URL Log Airflow (Sangat berguna untuk debug)
        str(exception)
    ]
    header = ['timestamp', 'dag_id', 'task_id', 'log_url', 'error_details']
    
    _write_csv(ERROR_LOG_PATH, row, header)
    logging.error(f"📝 Error Log saved to {ERROR_LOG_PATH}")

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
    """
    # 1. Catat ke Audit Log (Sejarah Lengkap)
    write_audit_log(context, "FAILED")
    
    # 2. Catat ke Error Log (Fokus Masalah)
    write_error_log(context)

    # 3. Kirim Notifikasi Discord
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    log_url = ti.log_url
    exception = context.get('exception')

    payload = {
        "content": f"🚨 **CRITICAL ALERT** <@{DISCORD_USER_ID}>! Pipeline Failed!",
        "embeds": [{
            "title": "❌ ETL Task Failed",
            "color": 15548997, # Red
            "fields": [
                {"name": "DAG", "value": f"`{dag_id}`", "inline": True},
                {"name": "Task", "value": f"`{task_id}`", "inline": True},
                {"name": "Error", "value": f"```{str(exception)[:200]}...```", "inline": False}
            ],
            "description": f"[👉 Click here to fix]({log_url})",
            "footer": {"text": "Details saved to logs/pipeline_error_log.csv"}
        }]
    }
    send_discord_message(payload)

def send_success_alert(context):
    """
    Called when a Task SUCCEEDS.
    """
    # Hanya catat ke Audit Log (Sukses tidak perlu masuk error log)
    write_audit_log(context, "SUCCESS")

    # Kirim Notifikasi Discord
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