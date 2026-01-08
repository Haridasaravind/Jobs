import requests
from bs4 import BeautifulSoup
import time
import re
import os
import hashlib
import psycopg2
import json
import html
import unicodedata
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# LOAD ENV
# =========================
load_dotenv()

# =========================
# CONFIG
# =========================
URL = "https://nvoids.com/search_sph.jsp"
SEARCH_TEXT = "java full stck developer"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "text/html"
}

# JAVA_KEYWORDS = [   "java", "spring boot","C2C" #]

JAVA_KEYWORDS = [
    "java",
    "java full stack",
    "full stack java",
    "spring boot",
    "java developer"
]

EXCLUDED_TITLE_KEYWORDS = [
    "lead",
    "architect",
    "solutions architect",
    "principal architect"
]

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
conn.autocommit = True
print("✅ Connected to Supabase")

# =========================
# SESSION WITH RETRY
# =========================
session = requests.Session()
retry = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
session.mount("https://", HTTPAdapter(max_retries=retry))

# =========================
# IN-RUN DUPLICATE TRACKER (UPDATED)
# =========================
SEEN_VENDOR_EMAILS = set()
DUPLICATE_VENDOR_COUNTS = {}

def is_duplicate_in_run(email):
    if not email:
        return False

    email = email.lower().strip()

    if email in SEEN_VENDOR_EMAILS:
        DUPLICATE_VENDOR_COUNTS[email] = DUPLICATE_VENDOR_COUNTS.get(email, 0) + 1
        return True

    SEEN_VENDOR_EMAILS.add(email)
    return False

# =========================
# CLEAN JOB TITLE
# =========================
def clean_job_title(title):
    title = title.lower()
    title = re.sub(r"\b(f2f|face[\s\-]?to[\s\-]?face)\b", "", title)
    title = re.sub(r"[\(\)\[\]\-_/]+", " ", title)
    title = re.sub(r"\s{2,}", " ", title)
    return title.strip().title()

# =========================
# EMAIL EXTRACTION
# =========================
def decode_cfemail(encoded):
    r = int(encoded[:2], 16)
    return "".join(
        chr(int(encoded[i:i+2], 16) ^ r)
        for i in range(2, len(encoded), 2)
    )

def extract_cfemails(soup):
    emails = set()
    for span in soup.select("span.__cf_email__"):
        encoded = span.get("data-cfemail")
        if encoded:
            try:
                emails.add(decode_cfemail(encoded))
            except Exception:
                pass
    return ", ".join(emails) if emails else None

def extract_emails_from_text(text):
    emails = set(re.findall(
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        text
    ))
    return ", ".join(emails) if emails else None

def filter_vendor_emails(email_string):
    if not email_string:
        return None
    blocked_domains = ("@nvoids.com", "@jobs.nvoids.com")
    valid = []
    for e in email_string.split(","):
        e = e.strip().lower()
        if not any(e.endswith(bd) for bd in blocked_domains):
            valid.append(e)
    return ", ".join(valid) if valid else None

# =========================
# FILTER LOGIC
# =========================
def is_valid_java_job(title, jd_text):
    text = f"{title.lower()} {jd_text.lower()}"
    if any(k in title.lower() for k in EXCLUDED_TITLE_KEYWORDS):
        return False
    return any(k in text for k in JAVA_KEYWORDS)

# =========================
# FETCH JD
# =========================
def fetch_and_parse_jd(url):
    r = session.get(url, headers=HEADERS, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    raw_email = extract_cfemails(soup) or extract_emails_from_text(soup.get_text())
    email = filter_vendor_emails(raw_email)

    jd_text = soup.get_text("\n", strip=True)
    if len(jd_text) < 150:
        return None, None

    return jd_text, email

# =========================
# Scrpper loop logic for lst 24 hours
# =========================
def is_recent_post(posted_text: str) -> bool:
    """
    Accepts:
    - Today
    - X hours ago
    - 1 day ago
    Rejects:
    - 2+ days ago
    """
    if not posted_text:
        return False

    text = posted_text.lower()

    if "today" in text:
        return True

    if "hour" in text:
        return True

    if "day" in text:
        match = re.search(r"\d+", text)
        if match:
            return int(match.group()) <= 1

    return False


# =========================
# SCRAPE SEARCH PAGE
# =========================
response = session.post(
    URL,
    headers=HEADERS,
    data={"txtSearch": SEARCH_TEXT},
    timeout=30
)
response.raise_for_status()

soup = BeautifulSoup(response.text, "html.parser")
rows = soup.find("table", attrs={"border": "1"}).find_all("tr")

total_jobs = len(rows)
java_filtered = 0
duplicates = 0
stored = 0

print(f"\n🟢 TOTAL JOBS FOUND: {total_jobs}\n")

for row in rows:
    cols = row.find_all("td")
    link = row.find("a")

    if not cols or not link:
        continue

    if len(cols) < 3:
        continue

    title = clean_job_title(cols[0].get_text(" ", strip=True))
    location = cols[1].get_text(strip=True)

    posted_text = cols[2].get_text(strip=True)

    # 🔴 FILTER: LAST 24 HOURS ONLY
    print(f"SKIPPED → {title} | POSTED: {posted_text}")
    # Temporry fix for the project 
    # if not is_recent_post(posted_text):
    #     continue

    # 🔴 QUICK JAVA TITLE FILTER
    if not any(k in title.lower() for k in JAVA_KEYWORDS):
        continue

    vendor = cols[3].get_text(strip=True) if len(cols) > 3 else "Unknown"
    url = "https://nvoids.com/" + link["href"]

    jd_text, email = fetch_and_parse_jd(url)

    if not jd_text or not is_valid_java_job(title, jd_text):
        continue

    java_filtered += 1

    if is_duplicate_in_run(email):
        duplicates += 1
        continue

    cursor.execute("""
        INSERT INTO java_jobs_v2 (
            job_title, job_location, vendor_name,
            vendor_email, job_description_text,
            source_url, jd_hash
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (jd_hash) DO NOTHING
    """, (
        title, location, vendor,
        email, jd_text,
        url, hashlib.sha256(jd_text.lower().encode()).hexdigest()
    ))

    stored += 1
    print(f"✅ Stored → {title} | {email}")
    time.sleep(2)

# =========================
# FINAL SUMMARY
# =========================
print("\n================ SUMMARY ================")
print(f"🟢 TOTAL JOBS FOUND     : {total_jobs}")
print(f"🟡 JAVA FILTERED JOBS   : {java_filtered}")
print(f"⛔ RUN DUPLICATES       : {duplicates}")
print(f"✅ STORED JOBS          : {stored}")
print("========================================")

# =========================
# TOP DUPLICATE VENDORS
# =========================
if DUPLICATE_VENDOR_COUNTS:
    print("\n🔥 TOP DUPLICATE VENDORS (Same Run)")
    print("---------------------------------")
    sorted_dupes = sorted(
        DUPLICATE_VENDOR_COUNTS.items(),
        key=lambda x: x[1],
        reverse=True
    )
    for idx, (email, count) in enumerate(sorted_dupes[:10], start=1):
        print(f"{idx}. {email}  →  {count} duplicates")
else:
    print("\n🎉 No duplicate vendors found in this run")

cursor.close()
conn.close()
