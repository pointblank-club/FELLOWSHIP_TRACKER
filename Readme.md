# Fellowship Tracker

An AI-powered scraper and tracker for fellowships, research internships, and open-source mentorship programs — built for CS students in Bangalore, India.

Gemini automatically generates search queries, crawls official program pages, extracts deadlines and eligibility details, stores everything in MongoDB, and serves it through a FastAPI + HTML frontend.

**Live:** [fellowship-tracker.vercel.app](https://fellowship-tracker.vercel.app)

---

## What It Does

1. **AI Query Generation** — Gemini generates targeted Google search queries for each program instead of using a hardcoded list
2. **Web Search** — Serper API searches Google for official program pages
3. **AI Link Filtering** — Gemini filters out blogs, aggregators, and social media — keeping only official application pages
4. **Page Crawling** — crawl4ai scrapes each page's content
5. **AI Data Extraction** — Gemini reads each page and extracts name, deadline, stipend, eligibility, mode, and tags
6. **MongoDB Storage** — All data is upserted into MongoDB Atlas
7. **Frontend** — Terminal-aesthetic UI with search, filters, and live stats

---

## Programs Tracked

| Category | Programs |
|---|---|
| Open Source | LFX Mentorship, GSoC, DWoC, KWoC, CNCF Mentorship, FOSS United Fellowship |
| Bitcoin / Web3 | Summer of Bitcoin |
| Research | SRFP (JNCASR), SRIP (IIT Gandhinagar), IIT SURGE / SPARK / SRF, MSR India |
| Scholarships | Reliance Foundation, Grace Hopper Celebration (GHC), LIFT Fellowship |
| + AI suggested | 5 additional programs per run based on your student profile |

---

## Project Structure

```
Fellowship_Tracker/
├── scraper/
│   ├── main.py              # AI scraper pipeline (runs locally)
│   └── requirements.txt     # Scraper-only dependencies (not deployed)
├── api/
│   └── index.py             # FastAPI server + serves frontend
├── index.html               # Frontend UI
├── requirements.txt         # API-only dependencies (deployed to Vercel)
├── vercel.json              # Vercel deployment config
├── .vercelignore            # Excludes scraper from Vercel bundle
└── .env                     # API keys (never commit this)
```

> **Why two `requirements.txt` files?** Vercel has a 500MB Lambda size limit. `crawl4ai` + `playwright` alone are ~400MB and are only needed for scraping locally. The deployed API only reads from MongoDB — it never scrapes.

---

## Setup

### Prerequisites
- Python 3.10+
- MongoDB Atlas account (free tier works)
- Serper API key → [serper.dev](https://serper.dev) (2500 free searches)
- Gemini API key → [aistudio.google.com](https://aistudio.google.com) (free tier)

### 1. Clone the repo

```bash
git clone https://github.com/DuttaNeel07/FELLOWSHIP_TRACKER.git
cd FELLOWSHIP_TRACKER
```

### 2. Create virtual environment

```bash
python -m venv venv

# Mac/Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 3. Install dependencies

```bash
# API dependencies
pip install -r requirements.txt

# Scraper dependencies (local only)
pip install -r scraper/requirements.txt
playwright install chromium
```

### 4. Create `.env` file

```env
MONGO_URL=mongodb+srv://your_connection_string
SERPER_API_KEY=your_serper_key
GEMINI_API_KEY=your_gemini_key
```

---

## Running Locally

### Step 1 — Run the scraper (populates MongoDB)

```bash
python scraper/main.py
```

This will take 20–60 minutes on the free Gemini tier due to rate limiting. The scraper handles this automatically with exponential backoff — just let it run.

### Step 2 — Start the API + frontend

```bash
uvicorn api.index:app --reload --port 8000
```

Open **http://localhost:8000** in your browser.

> You do NOT need to run `python -m http.server`. FastAPI serves the frontend directly.

### WhatsApp alerts

The scraper can notify the WhatsApp bot when it inserts a new opportunity or when an existing opportunity changes from closed to open. Configure these variables in `.env`:

```env
WHATSAPP_ALERT_URL=http://127.0.0.1:8082/fellowship-alert
WHATSAPP_ALERT_SECRET=the_same_secret_used_by_the_whatsapp_bot
```

The bot must be running and reachable at that URL. Alerts are authenticated with the `X-Fellowship-Alert-Secret` header, and the bot deduplicates repeated deliveries using the supplied idempotency key.

---

## Deploying to Vercel

### 1. Create the required files

**`requirements.txt`** (root — API only, no scraper packages):
```
fastapi
motor
dnspython
python-dotenv
pydantic
uvicorn
httpx
```

**`scraper/requirements.txt`** (local only — never deployed):
```
crawl4ai
playwright
google-genai
httpx
python-dotenv
motor
dnspython
```

**`.vercelignore`** (prevents scraper from being bundled):
```
scraper/
venv/
__pycache__/
*.pyc
```

### 2. Add environment variables in Vercel

Go to your project on [vercel.com](https://vercel.com) → **Settings → Environment Variables** and add:

```
MONGO_URL
SERPER_API_KEY
GEMINI_API_KEY
```

### 3. Push to GitHub

```bash
git add .
git commit -m "update"
git push
```

Vercel auto-deploys on every push. The `vercel.json` routes all traffic through FastAPI:

```json
{
  "version": 2,
  "rewrites": [
    { "source": "/api/(.*)", "destination": "/api/index.py" },
    { "source": "/(.*)",     "destination": "/api/index.py" }
  ]
}
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Frontend UI |
| GET | `/api/fellowships` | All opportunities (supports `?tag=`, `?open=true`, `?search=`, `?limit=`) |
| GET | `/api/stats` | Total, open, and deadline counts |
| GET | `/api/tags` | All distinct tags in the database |

---

## Rate Limits

The free Gemini tier allows ~15 requests/minute. The scraper sleeps 35 seconds between AI calls and retries with exponential backoff on 429 errors. To remove rate limits entirely, add billing to your Google AI Studio project — cost is under $0.01 per full scraper run.

To change the Gemini model, edit line 36 of `scraper/main.py`:

```python
GEMINI_MODEL = "models/gemini-2.0-flash-lite"
```

Available models confirmed for this project: `models/gemini-2.5-flash`, `models/gemini-2.0-flash`, `models/gemini-2.0-flash-lite`

---

## Customising for Your Profile

Edit `STUDENT_PROFILE` and `MUST_HAVE_PROGRAMS` at the top of `scraper/main.py` to target different programs or locations.

```python
STUDENT_PROFILE = {
    "location": "Bangalore, Karnataka, India",
    "education": "B.Tech / B.E. (undergraduate) or M.Tech (postgraduate)",
    "domains": ["computer science", "software engineering", "AI/ML", "open source", "research"],
    "year": "2025-2026 cycle",
}
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| AI | Google Gemini 2.0 Flash Lite |
| Search | Serper API (Google Search) |
| Scraping | crawl4ai + Playwright |
| Database | MongoDB Atlas |
| Backend | FastAPI + Motor (async) |
| Frontend | Vanilla HTML/CSS/JS + Tailwind CDN |
| Deployment | Vercel |