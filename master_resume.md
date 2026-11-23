# MASTER RESUME — Kaleru Yuvaraj
<!--
HOW THIS FILE IS USED
- The job bot hands this file to Claude every time you tap "Apply" in Telegram.
  Claude may ONLY use facts written here. It picks, reorders and rewords them for each job.
- Anything containing {{FILL ...}} is ignored by the bot until you replace it.
- Add facts freely. Keep each bullet true. Numbers you can defend in an interview are gold.
- Lines starting with "Priority:" tell the bot what to show first (high / medium / low).
-->

## CONTACT
- Name: Kaleru Yuvaraj
- Location: Hyderabad, Telangana, India
- Email: yuvaraj.kaleru.dev@gmail.com
- Phone: +91 9392716766
- LinkedIn: https://www.linkedin.com/in/yuvarajkaleru
- GitHub: https://github.com/yuvarajkaleru

## TARGET
- Roles: Python Backend Developer, FastAPI Developer, Python Full Stack Developer (fresher), Software Engineer Trainee, Backend / Python Intern
- Status: Final-year B.Tech student, graduating 2027. Fresher (0 years full-time experience). Available for internships now and full-time roles from 2027.
- Work location: Hyderabad only (on-site or hybrid)

## EDUCATION
- B.Tech, Computer Science & Business Systems (CSBS) — GITAM University, Hyderabad | 2023 – 2027 (expected) | CGPA: 7.45/10
- Intermediate (Class XII, MPC) — Narayana Junior College, Hyderabad | 2023 | 70.01%
- SSC (Class X) — Joshi's Vidhyaniketan School, Uppal, Telangana | 2021 | 95%

## TECHNICAL SKILLS (only what I can explain in an interview)
- Languages: Python (strong), JavaScript, SQL, Java (basics/OOP), C (basics), Bash (basics)
- Backend & APIs: FastAPI, RESTful APIs, CRUD, Pydantic v2 (field/model validators, computed fields, nested models, serialization), SQLAlchemy ORM, Swagger/OpenAPI, Uvicorn
- Auth & Security: JWT (python-jose, HS256), OAuth2 password flow, bcrypt password hashing (passlib), role-based access control (RBAC)
- Databases: PostgreSQL, MySQL, SQLite
- Frontend: HTML5, CSS3, JavaScript (Fetch API, DOM, localStorage), responsive UI
- AI & Data: Pandas, NumPy, Streamlit, Plotly, Prophet, time-series forecasting, anomaly detection, machine learning basics, NLP basics
- Cloud & Tools: Git, GitHub, Docker (basics, Dockerfile / docker-compose), AWS (basics), Linux, Vercel, VS Code, Postman
- Automation & AI tooling: n8n workflows, Model Context Protocol (MCP) server in Node.js
- Core concepts: Data Structures & Algorithms, Object-Oriented Programming

## EXPERIENCE
### AI/ML Intern (Python) — Infosys Springboard, Internship 6.0 (FitPulse Program) | Nov 2025 – Jan 2026 | Remote
Priority: high
Repo: https://github.com/yuvarajkaleru/Fitpulse-Health-Detection
Certificate: Infosys Springboard Certificate of Completion — "FitPulse: Health Anomaly Detection from Fitness Devices" (4 Nov 2025 – 10 Jan 2026)
- Built FitPulse, a Streamlit health-analytics dashboard, delivered in milestone-based modules (ingestion, cleaning, forecasting, anomaly detection, reporting).
- Built a Python pipeline with Pandas and NumPy that cleans and merges 2,881 records across 30 days of heart-rate, step-count, stress-level and calories-burned data.
- Implemented heart-rate anomaly detection using IQR-filtered baselines plus trailing rolling statistics, a persistence rule (consecutive readings) and Mild/Moderate/Critical severity tiers; identified 4 anomalies in the dataset.
- Implemented time-series forecasts of heart rate, sleep and steps with Prophet (weekly seasonality for sleep).
- Added a 0–100 wellness score, activity/lifestyle classification, trend visualisations across 4 health metrics, and CSV report export.

## PROJECTS
### Customer Support Ticket Management System — FastAPI, SQLAlchemy, SQLite, JWT, HTML/CSS/JS, Vercel | Completed (Nov 2026)
Priority: high
Repo: https://github.com/yuvarajkaleru/support-ticket-api
Live: https://support-ticket-api-eosin.vercel.app/
- Built a full-stack ticket management app with a FastAPI REST backend (12 API endpoints) for registration, login, tickets, agent assignment and comments, with Pydantic request/response schemas and auto-generated Swagger/OpenAPI docs.
- Implemented JWT authentication (OAuth2 password flow, 30-minute token expiry) and bcrypt password hashing; 10 endpoints are JWT-protected.
- Designed role-based access control for 3 roles (admin, agent, customer): admins see all tickets and assign agents, agents see assigned tickets, customers see only their own.
- Modelled Users, Tickets and Comments with SQLAlchemy ORM relationships on SQLite, with ticket status and priority tracking.
- Added title search, status/priority filters, newest-first date sorting and offset/limit pagination on ticket listing.
- Built a responsive vanilla HTML/CSS/JavaScript frontend with role-aware UI, a ticket detail panel and comments, and deployed it live on Vercel.

### AI-Powered Interview Platform — FastAPI, PostgreSQL, OpenAI/Gemini API, HTML/JS | Ongoing (design stage, final-year project)
Priority: medium
- Designing an AI interview platform that takes a job description and a resume as inputs and checks how the candidate's skills match the JD.
- Planned flow: for matched skills, look for evidence in the projects/links on the resume, then ask the candidate evidence-based questions to verify each claimed skill.
- Planned output: a final evaluation score and report for the candidate.

### GitHub Project Reviewer — FastAPI, Pydantic, React (Vite, Tailwind), Docker | Dec 2025
Priority: medium
Repo: https://github.com/yuvarajkaleru/github-project-reviewer-agent
- Built a FastAPI service (analyze, review, health and stats endpoints) that fetches repository metadata, file tree, commits, issues, releases and workflows from GitHub.
- Designed a five-step review pipeline (plan → fetch → analyze → synthesize → output) that scores repositories 0–100 on documentation, code quality, maturity, open-source readiness and resume appeal (rule-based scoring).
- Built a React frontend with score cards and a tabbed review display; containerised backend and frontend with Docker and docker-compose.

### Patient Records REST API — FastAPI, Pydantic v2 | Oct–Nov 2026
Priority: medium
Repo: https://github.com/yuvarajkaleru/FastAPI_Code
- Built CRUD endpoints (create, view, search, sort, edit, delete) with path/query parameter validation and proper status codes (201/400/404).
- Used Pydantic v2 field constraints and computed fields to derive BMI and a health verdict automatically.
- Practised advanced Pydantic: field validators, model validators, nested models and serialization.

### AI Automation Workflows — n8n | Dec 2025
Priority: low
Repo: https://github.com/yuvarajkaleru/ai-automation-n8n-workflows
- Built four n8n automation workflows: an AI agent with vector memory, a blog analyser, a restaurant image generator and a text-to-video maker.

### GitHub MCP Server — Node.js | Dec 2025
Priority: low
Repo: https://github.com/yuvarajkaleru/github-mcp-server
- Built a Model Context Protocol server that exposes GitHub repository data as AI-callable tools.

### JDBC SQLite Pipeline — Java, JDBC, SQLite | Jul 2026
Priority: low
Repo: https://github.com/yuvarajkaleru/JDBC-SQLite-Pipeline
- Java console app performing CRUD operations on SQLite through JDBC.

## CERTIFICATIONS
- Certificate of Completion, Internship 6.0 — FitPulse: Health Anomaly Detection from Fitness Devices — Infosys Springboard (issued Mar 2026)
- Front-End Web Development — E-Cell, IIT Roorkee (2024): HTML, CSS and JavaScript
- AWS Basics & Linux — KodeKloud (2025): hands-on cloud and Linux command-line labs

## ACHIEVEMENTS / EXTRAS
- Inclusion Officer (Core Team) — GITAM GitHub Club (2024 – 2025)

## NOTES FOR THE TAILORING BOT (not printed on the resume)
- I am a fresher. Never invent full-time work experience, years of experience, employers, metrics or skills.
- The Infosys Springboard role is an internship. List it under Experience as an internship, never as a job.
- The ticket system uses SQLite. Say "PostgreSQL" only in the skills section, never as that project's database.
- The AI-Powered Interview Platform is NOT built yet. Describe it only as "ongoing / in design" with planned features. Never say it parses resumes, uses embeddings, runs voice interviews or produces reports today.
- GitHub Project Reviewer scoring is rule-based. Never call it an LLM agent.
- Keep the resume to one page. Prefer the ticket system and the Infosys internship for backend/FastAPI roles.
