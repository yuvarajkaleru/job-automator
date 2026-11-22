import os
import sqlite3
import subprocess
import asyncio
import re
import time
import pandas as pd
from jobspy import scrape_jobs
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Bot, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 1. Database Setup
conn = sqlite3.connect("jobs.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        title TEXT,
        company TEXT,
        url TEXT,
        description TEXT,
        status TEXT
    )
""")
conn.commit()

RESUME_TEXT = """
Kaleru Yuvaraj Computer Science & Business Systems B.Tech 2027 expected GITAM University Hyderabad India.
Backend Developer Python FastAPI REST API Pydantic JWT RBAC PostgreSQL SQLAlchemy SQLite Docker AWS Vercel Linux.
Frontend HTML5 CSS3 JavaScript Responsive Web UI. AI ML Pandas NumPy Streamlit.
Projects: Customer Support Ticket Management System, AI Interview Platform, FitPulse Health Analytics.
Certifications: Front-End Web Development E-Cell IIT Roorkee, AWS Basics & Linux KodeKloud.
"""

CORE_SKILLS = [
    "python", "fastapi", "rest api", "pydantic", "jwt", "rbac", 
    "postgresql", "sqlalchemy", "sqlite", "docker", "aws", 
    "html", "css", "javascript", "pandas", "numpy", "streamlit"
]

# 2. Strict Fresher & Hierarchy Validation Engine
def evaluate_job(job_title, job_description, location_str):
    title_lower = job_title.lower()
    desc_lower = job_description.lower()
    loc_lower = location_str.lower()

    if not ("hyderabad" in loc_lower or "remote" in loc_lower or "india" in loc_lower):
        return 0.0, "Outside Location", [], "Excluded"

    senior_terms = [
        "senior", "sr.", "sr", "lead", "principal", "manager", 
        "architect", "staff", "head of", "director", "tech lead"
    ]
    for term in senior_terms:
        if re.search(r'\b' + re.escape(term) + r'\b', title_lower):
            return 0.0, "Senior Title Excluded", [], "Excluded"

    exp_patterns = [
        r'2\+\s*years', r'3\+\s*years', r'4\+\s*years', r'5\+\s*years', 
        r'2\s*to\s*\d\s*years', r'3\s*to\s*\d\s*years', r'minimum\s*2\s*years', r'minimum\s*3\s*years'
    ]
    if any(re.search(pattern, desc_lower) for pattern in exp_patterns):
        return 0.0, "Experience Exceeded", [], "Excluded"

    role_type = "2nd Pref: Internship" if "intern" in title_lower or "internship" in title_lower else "1st Pref: Fresher Full-Time"
    pref_boost = 0.30 if role_type == "1st Pref: Fresher Full-Time" else 0.15

    matched_skills = [skill for skill in CORE_SKILLS if re.search(r'\b' + re.escape(skill) + r'\b', desc_lower)]
    
    similarity = 0.3
    if len(job_description.strip()) > 30:
        try:
            vectorizer = TfidfVectorizer(stop_words='english')
            tfidf_matrix = vectorizer.fit_transform([RESUME_TEXT, job_description])
            similarity = float(cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0])
        except Exception:
            similarity = 0.3

    final_score = min(1.0, similarity + pref_boost + (len(matched_skills) * 0.03))
    return round(final_score * 100, 1), "Valid Role", matched_skills, role_type

# 3. Rate-Limit Resistant Scraper Engine (Indeed + Google/LinkedIn with Delays)
def fetch_filtered_jobs():
    print("Scraping fresh job listings with rate-limit delays...")
    all_jobs = []
    
    search_queries = [
        {"term": "Python Developer", "location": "Hyderabad, India"},
        {"term": "FastAPI Developer", "location": "Remote"},
        {"term": "Python Intern", "location": "Hyderabad, India"},
        {"term": "Graduate Engineer Trainee Python", "location": "India"}
    ]

    for q in search_queries:
        try:
            print(f"Fetching: '{q['term']}' in '{q['location']}'...")
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin"],
                search_term=q["term"],
                location=q["location"],
                results_wanted=6,
                hours_old=72,
                country_indeed='india',
                fetch_description=True
            )
            if not jobs.empty:
                all_jobs.append(jobs)
            time.sleep(2.5)  # Pause to avoid HTTP 429 rate limits
        except Exception as e:
            print(f"Notice for '{q['term']}': {e}")

    if not all_jobs:
        return pd.DataFrame()

    df = pd.concat(all_jobs, ignore_index=True)
    df.drop_duplicates(subset=['job_url'], inplace=True)

    scored_jobs = []
    for _, row in df.iterrows():
        title = str(row.get('title', ''))
        desc = str(row.get('description', ''))
        location = str(row.get('location', 'Hyderabad, India'))
        
        score, status, matched_skills, role_type = evaluate_job(title, desc, location)
        
        if score >= 35.0:
            row_dict = row.to_dict()
            row_dict['match_score'] = score
            row_dict['role_type'] = role_type
            row_dict['matched_skills'] = ", ".join(matched_skills[:6]).upper() if matched_skills else "PYTHON, FASTAPI"
            scored_jobs.append(row_dict)

    scored_df = pd.DataFrame(scored_jobs)
    if not scored_df.empty:
        scored_df.sort_values(by='match_score', ascending=False, inplace=True)
    return scored_df

# 4. Resume Generator (Claude Pro CLI)
def generate_tailored_resume(job_title, company, description):
    master_resume = ""
    if os.path.exists("master_resume.md"):
        with open("master_resume.md", "r") as f:
            master_resume = f.read()

    prompt = f"""
    You are an expert technical recruiter tailoring a resume for Kaleru Yuvaraj (2027 CSBS Graduate at GITAM University, Hyderabad).
    Target Role: '{job_title}' at '{company}'
    Job Description: {description}
    Master Resume: {master_resume}

    Instructions:
    - Keep output strictly in clean Markdown format.
    - Highlight core tech stack: Python, FastAPI, Pydantic, REST APIs, JWT/RBAC, PostgreSQL, HTML5/CSS3/JavaScript, and AWS.
    - Emphasize the Ticket Management System (FastAPI + JWT + HTML/CSS/JS + Vercel) and AI Interview Platform.
    - Frame experience for entry-level, junior, or internship applications without inventing experience.
    """

    env_vars = os.environ.copy()
    if "CLAUDE_CODE_TOKEN" in env_vars:
        env_vars["CLAUDE_CODE_OAUTH_TOKEN"] = env_vars["CLAUDE_CODE_TOKEN"]

    result = subprocess.run(
        ["claude", "-p", prompt, "--dangerously-skip-permissions"],
        capture_output=True,
        text=True,
        env=env_vars
    )
    return result.stdout

# 5. Telegram Callback Listener
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data_parts = query.data.split(":")
    action, job_id = data_parts[0], data_parts[1]

    if action == "app":
        await query.edit_message_text(text="⏳ **Tailoring resume for Yuvaraj with Claude Pro...**", parse_mode="Markdown")
        cursor.execute("SELECT title, company, url, description FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        
        if row:
            title, company, url, desc = row
            tailored_md = generate_tailored_resume(title, company, desc)

            filename = f"Resume_Yuvaraj_{company.replace(' ', '_')}.md"
            with open(filename, "w") as f:
                f.write(tailored_md)

            with open(filename, "rb") as doc:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=doc,
                    caption=f"✅ **Tailored Resume Ready!**\n\n📌 **Role:** {title} at {company}\n🔗 [Direct Apply Link]({url})",
                    parse_mode="Markdown"
                )
            cursor.execute("UPDATE jobs SET status = 'approved' WHERE id = ?", (job_id,))
            conn.commit()

    elif action == "rej":
        await query.edit_message_text(text="❌ **Job Rejected.**", parse_mode="Markdown")
        cursor.execute("UPDATE jobs SET status = 'rejected' WHERE id = ?", (job_id,))
        conn.commit()

async def main():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("ERROR: Missing secrets.")
        return

    bot = Bot(token=TELEGRAM_TOKEN)
    df = fetch_filtered_jobs()

    if not df.empty:
        sent_count = 0
        for _, row in df.iterrows():
            job_url = str(row.get('job_url', ''))
            job_id = str(row['id']) if 'id' in row and pd.notna(row['id']) else str(hash(job_url))
            
            cursor.execute("SELECT id FROM jobs WHERE id = ?", (job_id,))
            if cursor.fetchone():
                continue

            sent_count += 1
            title = str(row.get('title', 'Junior Developer'))
            company = str(row.get('company', 'Company'))
            location = str(row.get('location', 'Hyderabad / Remote'))
            site = str(row.get('site', 'Job Board')).capitalize()
            score = row.get('match_score', 0)
            role_type = row.get('role_type', '1st Pref: Fresher Full-Time')
            skills = row.get('matched_skills', 'PYTHON, FASTAPI')
            description = str(row.get('description', ''))

            cursor.execute(
                "INSERT INTO jobs (id, title, company, url, description, status) VALUES (?, ?, ?, ?, ?, ?)",
                (job_id, title, company, job_url, description, 'pending')
            )
            conn.commit()

            msg_text = (
                f"🌟 *[{role_type}]*\n"
                f"🎯 *Match Rating: {score}%*\n"
                f"⚡ *{title}*\n"
                f"🏢 *Company:* {company}\n"
                f"📍 *Location:* {location}\n"
                f"🌐 *Source:* {site}\n"
                f"🛠 *Matched Skills:* `{skills}`\n\n"
                f"🚀 [1-Click Direct Apply ({site})]({job_url})"
            )
            
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("❇️ Approve & Tailor", callback_data=f"app:{job_id}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"rej:{job_id}")
                ]
            ])
            await bot.send_message(chat_id=CHAT_ID, text=msg_text, parse_mode="Markdown", reply_markup=keyboard)

        print(f"Sent {sent_count} fresh roles to Telegram.")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await asyncio.sleep(300)
    await app.updater.stop()
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
