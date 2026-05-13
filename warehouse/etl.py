print("ETL PIPELINE STARTED")

from warehouse.db import get_conn
from warehouse.load_fact import run as load_staging_step


def run_sql(file_path):
    print(f"\nRunning SQL file: {file_path}")

    conn = get_conn()
    cur = conn.cursor()

    try:
        with open(file_path, "r") as f:
            sql = f.read()

        cur.execute(sql)
        conn.commit()

        print(f"Finished: {file_path}")

    except Exception as e:
        conn.rollback()
        print(f"Error in {file_path}:", e)

    finally:
        cur.close()
        conn.close()


def run():
    print("\n🔹 STEP 1: Create Schema")
    run_sql("warehouse/schema.sql")

    print("\n🔹 STEP 2: Load STAGING from GOLD")
    load_staging_step()

    print("\n🔹 STEP 3: Transform (DIM + FACT)")
    run_sql("warehouse/sql/transform.sql")

    print("\n🔹 STEP 4: Validate")
    run_sql("warehouse/sql/validate.sql")

    print("\nETL COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    run()