import os
import re
import psycopg2
from dotenv import load_dotenv
from gmail import get_gmail_service, create_html_draft

# =========================
# LOAD ENV
# =========================
load_dotenv()

# =========================
# CONFIG
# =========================
# 
MAX_DRAFTS = 10
CC_EMAIL = "tulasi@stemsolllc.com"

# =========================
# DB CONNECTION 
# =========================
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
    sslmode="require"
)
cursor = conn.cursor()

# =========================
# GMAIL AUTH
# =========================
gmail = get_gmail_service()
print("✅ Gmail authenticated")

# =========================
# UTILS
# =========================
def clean_jd_text(text: str) -> str:
    """
    Removes from JD:
    - URLs
    - Real emails
    - Masked emails
    - [protected], email protected, EMAIL PROTECTED
    - Email labels
    """
    if not text:
        return ""

    # Normalize first
    text = text.replace("\xa0", " ")

    # Remove URLs
    text = re.sub(r'https?://\S+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'www\.\S+', '', text, flags=re.IGNORECASE)

    # 🔥 REMOVE [protected] & email protected explicitly
    text = re.sub(
        r'\[?\s*(email\s*)?protected\s*\]?',
        '',
        text,
        flags=re.IGNORECASE
    )

    # Remove real & masked emails
    text = re.sub(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
        '',
        text,
        flags=re.IGNORECASE
    )

    # Remove email labels
    text = re.sub(
        r'\b(email|email id|e-mail|contact email)\b\s*[:\-]?\s*',
        '',
        text,
        flags=re.IGNORECASE
    )

    # Cleanup formatting
    text = re.sub(r'\n{2,}', '\n', text)
    text = re.sub(r'[ \t]{2,}', ' ', text)

    return text.strip()


def jd_to_html_clean(jd_text: str) -> str:
    jd_text = clean_jd_text(jd_text)

    lines = [
        line.strip()
        for line in jd_text.splitlines()
        if line.strip()
    ]

    return "<br>".join(lines)

# =========================
# FETCH JOBS
# =========================
cursor.execute("""
    SELECT
        id,
        job_title,
        job_location,
        vendor_email,
        source_url,
        job_description_text
    FROM java_jobs_v2
    WHERE email_draft_created = FALSE
    ORDER BY created_at
    LIMIT %s;
""", (MAX_DRAFTS,))

rows = cursor.fetchall()
print(f"🟢 Jobs to draft: {len(rows)}")

# =========================
# CREATE DRAFTS
# =========================
for row in rows:
    job_id, title, location, vendor_email, source_url, jd_text = row

    if not vendor_email:
        print(f"⚠️ Skipping (no vendor email): {title}")
        continue

    print(f"✉️ Drafting: {title}")

    jd_html = jd_to_html_clean(jd_text or "")

    html_body = f"""
    <p>Hello ,</p>
    <p>I hope you are doing well.</p>
    <p>
        I recently came across your posting for a
        <b>Java Full Stack Developer</b> role and wanted to
        share my resume for your consideration.

    </p>
    <p>
        <b>Visa:</b> H1B<br>
        <b>Location:</b> Local
    </p>
    <p>
        Please let me know if you need any additional information.
        I look forward to hearing from you.
    </p>
    <p>
        <br> Regards, </br>
        <br> Aravind Haridas </br> 
        <br> ✉️ aravind.har0603@gmail.com | 📞 +1 857-693-9910 </br>
    </p>
    <hr>
    <p>
        <b>Job Posting Link:</b><br>
        <a href="{source_url}" target="_blank">{source_url}</a>
    </p>
    <p><b>Job Description:</b></p>
    <p style="font-size:12px; color:#333;">
        {jd_html}
    </p>
    """

    create_html_draft(
        gmail,
        to=vendor_email,
        cc=CC_EMAIL,
        subject=f"Application – {title}",
        html_body=html_body
    )

    cursor.execute("""
        UPDATE java_jobs_v2
        SET email_draft_created = TRUE
        WHERE id = %s
    """, (job_id,))

# =========================
# CLEANUP
# =========================
conn.commit()
cursor.close()
conn.close()
print("🎉 ALL DRAFTS CREATED SUCCESSFULLY")
