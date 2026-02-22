import smtplib
import os
import socket
import time
import threading
from queue import Queue, Empty
from supabase import create_client
import dns.resolver
import sys

# ===== Live logs for GitHub =====
sys.stdout.reconfigure(line_buffering=True)
print("Script started")

# ======================
# Supabase Config
# ======================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ======================
# Runtime limit (GitHub safe)
# ======================
START_TIME = time.time()
MAX_RUNTIME = 60 * 60 * 5  # 60(seconds) 60(minutes) 5(hours)
# 

# ======================
# Queue + Fetch Lock
# ======================
email_queue = Queue()
fetch_lock = threading.Lock()

# ======================
# MX lookup
# ======================
def get_mx_records(domain):
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        return sorted([str(r.exchange).rstrip('.') for r in answers])
    except:
        return []

# ======================
# SMTP verification
# ======================
def verify_email_smtp(email):
    domain = email.split('@')[1]
    mx_records = get_mx_records(domain)

    if not mx_records:
        return False

    for mx in mx_records:
        try:
            server = smtplib.SMTP(mx, timeout=10)
            server.helo()
            server.mail('test@example.com')
            code, message = server.rcpt(email)
            server.quit()

            if code == 250:
                return True

        except (
            smtplib.SMTPServerDisconnected,
            smtplib.SMTPConnectError,
            smtplib.SMTPHeloError,
            smtplib.SMTPRecipientsRefused,
            socket.error,
            smtplib.SMTPException
        ):
            continue

    return False

# ======================
# Fetch from Supabase
# ======================
def fetch_to_queue(batch_size=500):
    try:
        response = supabase.rpc(
            "get_emails_to_verify",
            {"p_table": "gmail", "p_limit": batch_size}
        ).execute()

        emails = response.data or []

        for row in emails:
            email_queue.put({
                "id": row["id"],
                "email": row["email"]
            })

        if emails:
            print(f"Fetched {len(emails)} emails")

        return len(emails)

    except Exception as e:
        print("Fetch error:", e)
        return 0

# ======================
# Update result
# ======================
def update_result(email_id, is_valid):
    try:
        supabase.table("gmail").update({
            "valid": is_valid,
            "status": "done"
        }).eq("id", email_id).execute()
    except Exception as e:
        print("Update error:", e)

# ======================
# Worker
# ======================
def worker(worker_id):
    print(f"Worker {worker_id} started")

    while True:
        # Stop when GitHub time limit reached
        if time.time() - START_TIME > MAX_RUNTIME:
            print(f"Worker {worker_id} stopping (time limit)")
            break

        # Refill queue safely (only one worker fetches)
        if email_queue.qsize() < 20:
            if fetch_lock.acquire(blocking=False):
                try:
                    fetch_to_queue()
                finally:
                    fetch_lock.release()

        # Get email task
        try:
            item = email_queue.get(timeout=5)
        except Empty:
            continue

        email_id = item["id"]
        email = item["email"]

        try:
            #print(f"[W{worker_id}] verifying {email}")

            is_valid = verify_email_smtp(email)

            if is_valid:
                print(f"[W{worker_id}] YES {email}")
            else:
                print(f"[W{worker_id}] NO {email}")

            update_result(email_id, is_valid)

            # Small delay to reduce blocking risk
            #time.sleep(1)

        except Exception as e:
            print(f"[W{worker_id}] error:", e)
            try:
                supabase.table("gmail").update({
                    "status": "failed"
                }).eq("id", email_id).execute()
            except:
                pass

        finally:
            email_queue.task_done()

# ======================
# Start Workers
# ======================
num_workers = 3

for i in range(num_workers):
    threading.Thread(
        target=worker,
        args=(i + 1,),
        daemon=True
    ).start()
    time.sleep(2)

# ======================
# Keep main alive
# ======================
while time.time() - START_TIME < MAX_RUNTIME:
    time.sleep(5)

print("Job finished")
