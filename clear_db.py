"""Clear all data from Postgres."""
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    exit(1)

print(f"Connecting to database...")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Deleting all data...")
cur.execute("DELETE FROM title_samples;")
cur.execute("DELETE FROM title_history;")
cur.execute("DELETE FROM videos;")
cur.execute("DELETE FROM channels;")
conn.commit()

print("Verifying...")
cur.execute("SELECT 'channels' as tbl, count(*) FROM channels UNION ALL SELECT 'videos', count(*) FROM videos UNION ALL SELECT 'title_samples', count(*) FROM title_samples UNION ALL SELECT 'title_history', count(*) FROM title_history;")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

cur.close()
conn.close()
print("Done!")
