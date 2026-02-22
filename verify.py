import smtplib
import socket
import time
import threading
from queue import Queue
from supabase import create_client
import dns.resolver

"""
pip install supabase dnspython
python cleanbounce_1.py
"""

# ======================
# Supabase Config
# ======================

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ======================
# Queue
# ======================
email_queue = Queue()

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
# Your SMTP verification
# ======================
def verify_email_smtp(email):
    domain = email.split('@')[1]
    mx_records = get_mx_records(domain)

    if not mx_records:
        return False

    for mx in mx_records:
        try:
            server = smtplib.SMTP(mx, timeout=10)
            server.set_debuglevel(0)
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
# Fetch from RPC
# ======================
def fetch_to_queue(batch_size=1000):
    response = supabase.rpc(
        "get_emails_to_verify",
        {"p_table": "gmail", "p_limit": batch_size}
    ).execute()

    emails = response.data or []

    for row in emails:
        email_queue.put({"id": row["id"], "email": row["email"]})

    #print(f"Fetched {len(emails)} emails")
    return len(emails)

# ======================
# Update result
# ======================
def update_result(email_id, is_valid):
    supabase.table("gmail").update({
        "valid": is_valid,
        "status": "done"
    }).eq("id", email_id).execute()

START_TIME = time.time()
MAX_RUNTIME = 60 * 50  # 50 minutes

# ======================
# Worker
# ======================
def worker(worker_id):
    while True:
        # Stop after max runtime
        if time.time() - START_TIME > MAX_RUNTIME:
            print("Time limit reached, stopping worker")
            break
          
        # Refill queue if empty
        if email_queue.empty():
            fetched = fetch_to_queue()
            if fetched == 0:
                print("No emails left. Waiting...")
                time.sleep(5)
                continue

        item = email_queue.get()
        email_id = item["id"]
        email = item["email"]

        try:
            print(f"Worker {worker_id} verifying:", email)

            is_valid = verify_email_smtp(email)
            if is_valid == True:
                print('yes: ', email)
            else:
                print('no: ', email)
            update_result(email_id, is_valid)

        except Exception as e:
            print(f"Worker {worker_id} error:", e)
            # Optional: mark failed
            supabase.table("gmail").update({
                "status": "failed"
            }).eq("id", email_id).execute()

        finally:
            email_queue.task_done()

# ======================
# Start Workers
# ======================
num_workers = 4

for i in range(num_workers):
    threading.Thread(target=worker, args=(i+1,), daemon=True).start()
    time.sleep(3)
  
# Keep main alive
while time.time() - START_TIME < MAX_RUNTIME:
    time.sleep(1)

print("Job finished")
