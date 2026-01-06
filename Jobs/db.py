import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
    sslmode="require"
)
conn.autocommit = True
cursor = conn.cursor()

def insert_job(job):
    cursor.execute("""
        INSERT INTO java_jobs (
            job_title,
            vendor_name,
            vendor_email,
            vendor_phone,
            job_location,
            job_description_html,
            job_description_text,
            source_url,
            jd_hash
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (jd_hash) DO NOTHING;
    """, (
        job["title"],
        job["vendor"],
        job["email"],
        job["phone"],
        job["location"],
        job["description_html"],
        job["description_text"],
        job["url"],
        job["hash"]
    ))

def fetch_jobs_without_drafts(limit=10):
    cursor.execute("""
        SELECT
            id,
            job_title,
            vendor_email,
            job_description_html
        FROM java_jobs
        WHERE email_draft_created = false
        ORDER BY created_at DESC
        LIMIT %s;
    """, (limit,))
    return cursor.fetchall()

def mark_job_as_drafted(job_id):
    cursor.execute(
        "UPDATE java_jobs SET email_draft_created = true WHERE id = %s;",
        (job_id,)
    )
