import psycopg2
from psycopg2 import OperationalError

def create_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="tanish",
            password="235711",
            host="localhost",
            port="5433"
        )
        print("✅ Connection to PostgreSQL successful")
        return conn
    except OperationalError as e:
        print("❌ Failed to connect to PostgreSQL")
        print("Error:", e)
        return None

def run_query():
    conn = create_connection()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        for _ in range(10):
            query = "SELECT * FROM advisor where i_id = '123';"
            cur.execute(query)
            conn.commit()
            rows = cur.fetchall()
            for row in rows:
                print(row)

        cur.close()
    except Exception as e:
        print("❌ Error executing query:", e)
    finally:
        conn.close()
        print("🔒 Connection closed")

if __name__ == "__main__":
    run_query()
