import os
import sqlite3
import subprocess
import asyncio
import pandas as pd
from jobspy import scrape_jobs
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Bot, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

print(f"DEBUG: TELEGRAM_TOKEN loaded: {bool(TELEGRAM_TOKEN)}")
print(f"DEBUG: CHAT_ID loaded: {CHAT_ID}")

# 1. Database Initialization
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

# 2. Optimized Scraper Engine
def fetch_top_jobs():
    print("Starting job search via JobSpy...")
    try:
        jobs = scrape_jobs(
            site_name=["linkedin", "indeed"],
            search_term="Software Engineer",
            location="United States",
            results_wanted=10,
            hours_old=72,
            country_indeed='usa'
        )
        print(f"Found {len(jobs)} total jobs.")
        return jobs
    except Exception as e:
        print(f"Scraper error: {e}")
        return pd.DataFrame()

# 3. Resume Tailoring via Claude Pro CLI
def generate_tailored_resume(job_title, company, description):
    master_resume = ""
    if os.path.exists("master_resume.md"):
        with open("master_resume.md", "r") as f:
            master_resume = f.read()

    prompt = f"""
    You are an expert technical recruiter and resume writer.
    Tailor this Master Resume for the role of '{job_title}' at '{company}'.
    
    Target Job Description:
    {description}

    Master Resume:
    {master_resume}

    Instructions:
    - Keep output purely in clean Markdown format.
    - Emphasize matching skills, keywords, and achievements relevant to this role.
    - Do not invent false experience.
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

# 4. Telegram Interactive Callbacks
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data_parts = query.data.split(":")
    action = data_parts[0]
    job_id = data_parts[1]

    if action == "app":
        await query.edit_message_text(text="⏳ **Tailoring resume with Claude Pro...**", parse_mode="Markdown")
        
        cursor.execute("SELECT title, company, url, description FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        
        if row:
            title, company, url, desc = row
            tailored_md = generate_tailored_resume(title, company, desc)

            filename = f"Resume_{company.replace(' ', '_')}.md"
            with open(filename, "w") as f:
                f.write(tailored_md)

            with open(filename, "rb") as doc:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=doc,
                    caption=f"✅ **Tailored Resume Ready!**\n\n📌 **Role:** {title} at {company}\n🔗 [Apply Directly]({url})",
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
        print("ERROR: Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID secrets.")
        return

    bot = Bot(token=TELEGRAM_TOKEN)
    df = fetch_top_jobs()

    if not df.empty:
        new_jobs_count = 0
        for _, row in df.iterrows():
            job_url = str(row.get('job_url', ''))
            job_id = str(row['id']) if 'id' in row and pd.notna(row['id']) else str(hash(job_url))
            
            cursor.execute("SELECT id FROM jobs WHERE id = ?", (job_id,))
            if cursor.fetchone():
                continue

            new_jobs_count += 1
            title = str(row.get('title', 'Software Engineer'))
            company = str(row.get('company', 'Unknown Company'))
            location = str(row.get('location', 'Remote'))
            description = str(row.get('description', ''))

            cursor.execute(
                "INSERT INTO jobs (id, title, company, url, description, status) VALUES (?, ?, ?, ?, ?, ?)",
                (job_id, title, company, job_url, description, 'pending')
            )
            conn.commit()

            msg_text = (
                f"📌 *{title}*\n"
                f"🏢 *Company:* {company}\n"
                f"📍 *Location:* {location}\n"
                f"🔗 [View Original Job Posting]({job_url})"
            )
            
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("❇️ Approve & Tailor", callback_data=f"app:{job_id}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"rej:{job_id}")
                ]
            ])
            
            await bot.send_message(chat_id=CHAT_ID, text=msg_text, parse_mode="Markdown", reply_markup=keyboard)

        print(f"Sent {new_jobs_count} new jobs to Telegram.")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    print("Listening for Telegram Approve/Reject clicks for 5 minutes...")
    await asyncio.sleep(300)
    await app.updater.stop()
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
