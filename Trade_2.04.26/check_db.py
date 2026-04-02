import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="1234",
    dbname="postgres"  # пробуем подключиться к системной БД
)

cur = conn.cursor()
cur.execute("SELECT datname FROM pg_database;")

for row in cur.fetchall():
    print(row[0])

cur.close()
conn.close()

import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="1234",
    dbname="trade_platform",
)
cur = conn.cursor()

queries = [
    "SELECT current_database();",
    "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;",
]

for q in queries:
    print("\nQUERY:", q)
    cur.execute(q)
    for row in cur.fetchall():
        print(row)

cur.close()
conn.close()

import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="1234",
    dbname="trade_platform",
)

cur = conn.cursor()

cur.execute("""
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'liquidation_1m'
ORDER BY ordinal_position;
""")

for row in cur.fetchall():
    print(row[0])

cur.close()
conn.close()

import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="1234",
    dbname="trade_platform",
)
cur = conn.cursor()

queries = [
    """
    SELECT MIN(open_time), MAX(open_time), COUNT(*)
    FROM candles
    WHERE interval = '5m';
    """,
    """
    SELECT MIN(bucket_ts), MAX(bucket_ts), COUNT(*)
    FROM liquidation_1m;
    """,
]

for q in queries:
    print("\nQUERY:")
    print(q.strip())
    cur.execute(q)
    print(cur.fetchall())

cur.close()
conn.close()