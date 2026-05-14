from warehouse.db import get_conn

def run():
    print("Validating data...")

    conn = get_conn()
    cur = conn.cursor()

    tables = ["dim_vendor", "dim_location", "dim_date", "fact_trips"]

    try:
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            count = cur.fetchone()[0]
            print(f"{t}: {count} rows")

        print("Validation completed")

    except Exception as e:
        print("Validation error:", e)

    finally:
        cur.close()
        conn.close()