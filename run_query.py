import psycopg2
from psycopg2 import OperationalError
import threading

def run_query(thread_id):
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="tanish",
            password="235711",
            host="localhost",
            port="5433"
        )
        print(f"[Thread {thread_id}] ✅ Connected to PostgreSQL")

        cur = conn.cursor()
        for _ in range(10):
            query = "SELECT * FROM advisor WHERE i_id = '123';"
            cur.execute(query)
            rows = cur.fetchall()
            print(f"[Thread {thread_id}] Got {len(rows)} rows")
            
        conn.commit()
        cur.close()
        conn.close()
        print(f"[Thread {thread_id}] 🔒 Connection closed")

    except OperationalError as e:
        print(f"[Thread {thread_id}] ❌ Failed to connect")
        print("Error:", e)
    except Exception as e:
        print(f"[Thread {thread_id}] ❌ Query failed:", e)

def main():
    threads = []
    num_threads = 10  # You can increase this if needed

    for i in range(num_threads):
        thread = threading.Thread(target=run_query, args=(i,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
