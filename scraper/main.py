import os
import re
import json
import asyncio
import hashlib
from collections import Counter
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse

import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, DefaultMarkdownGenerator, PruningContentFilter
from groq import AsyncGroq
from scraper.dates import calculate_is_open, has_known_deadline, parse_opportunity_date
from scraper.discord import send_discord_notification
from scraper.whatsapp import (
    classify_opportunity_event,
    make_idempotency_key,
    send_whatsapp_alert,
)

# ─────────────────────────── ENV SETUP ───────────────────────────
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

MONGO_URL  = os.getenv("MONGO_URL")
SERPER_KEY = os.getenv("SERPER_API_KEY")
GROQ_KEY    = os.getenv("GROQ_API_KEY")
WHATSAPP_ALERT_URL = os.getenv("WHATSAPP_ALERT_URL")
WHATSAPP_ALERT_SECRET = os.getenv("WHATSAPP_ALERT_SECRET")
# DRY_RUN=1 reads the database but never writes to it or sends Discord/WhatsApp messages.
DRY_RUN = os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes"}

mongo_client = AsyncIOMotorClient(MONGO_URL, serverSelectionTimeoutMS=5000)
db           = mongo_client.fellowship_tracker
collection   = db.fellowships
discovered_collection = db.discovered_links
skipped_collection = db.skipped_links

groq_client = AsyncGroq(api_key=GROQ_KEY)
ai_lock = asyncio.Lock()
stats = Counter()

GROQ_MODEL  = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

STUDENT_PROFILE = {
    "location": "Bangalore, Karnataka, India",
    "education": "B.Tech / B.E. (undergraduate) or M.Tech (postgraduate)",
    "domains": ["computer science", "software engineering", "AI/ML", "open source", "research"],
    "year": "2025-2026 cycle",
}

MUST_HAVE_PROGRAMS = [
    "LFX Mentorship (Linux Foundation)",
    "GSoC - Google Summer of Code",
    "DWoC - Delta Winter of Code",
    "KWoC - Kharagpur Winter of Code",
    "CNCF Mentorship Program",
    "Summer of Bitcoin",
    "FOSS United Fellowship",
    "Reliance Foundation Undergraduate Scholarship",
    "Grace Hopper Celebration (GHC) Scholarship",
    "LIFT Fellowship",
    "IIT Research Internship (SURGE / SPARK / SRF)",
    "SRFP - JNCASR Summer Research Fellowship",
    "SRIP - IIT Gandhinagar Summer Research Internship",
    "MSR - Microsoft Research India Fellowship",
]

BLACKLISTED_DOMAINS = {
    "instagram.com", "facebook.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com", "reddit.com",
    "quora.com", "medium.com", "t.co", "bit.ly",
}

DISCOVERY_QUERIES = [
    "computer science fellowship apply",
    "AI internship for students",
    "summer research program computer science",
    "undergraduate research internship India",
    "open source mentorship program",
    "engineering fellowship for students",
    "research internship Bangalore computer science",
    "remote AI fellowship students",
]

DISCOVERY_DOMAINS = [
    "https://iisc.ac.in",
    "https://iiit.ac.in",
    "https://research.google",
    "https://careers.microsoft.com",
    "https://cncf.io",
    "https://linuxfoundation.org",
    "https://mlh.io",
    "https://outreachy.org",
]

async def ask_ai(prompt: str, max_tokens: int = 2048) -> str:
    """Call Groq with automatic retry on rate limits."""
    if stats["ai_daily_limit"]:
        stats["ai_errors"] += 1  # counts as failed, so the page is read again next run
        return ""
    stats["ai_calls"] += 1
    for attempt in range(4):
        try:
            resp = await groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=0.2,
                response_format={"type": "json_object"},
                # Reasoning models think before answering; keep it short so the JSON answer fits.
                extra_body={"reasoning_effort": "low"} if "gpt-oss" in GROQ_MODEL else None,
            )
            return (resp.choices[0].message.content or "").strip()
        except Exception as e:
            err = str(e)
            if "429" in err or "rate_limit" in err.lower():
                if "per day" in err:
                    # Waiting won't help until tomorrow; the pages not checked yet are picked up next run.
                    print(f"Groq daily limit reached for {GROQ_MODEL}. Remaining pages will be checked next run.")
                    stats["ai_daily_limit"] = 1
                    break
                # Groq says how long until the per-minute token budget frees up.
                retry_after = getattr(getattr(e, "response", None), "headers", {}).get("retry-after")
                wait = min(float(retry_after), 60) if retry_after else (2 ** attempt) * 5
                print(f"  ⏳ Rate limited. Waiting {wait:.0f}s...")
                await asyncio.sleep(wait)
            else:
                print(f"Groq error: {err[:300]}")
                stats["ai_errors"] += 1
                return ""
    stats["ai_errors"] += 1
    return ""


def safe_parse_json(raw: str):
    try:
        cleaned = re.sub(r"```json|```", "", raw).strip()
        match = re.search(r'(\{.*\}|\[.*\])', cleaned, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception:
        pass
    return None

async def get_existing_urls() -> tuple[set, set]:
    """Fetch all URLs already stored in MongoDB, and the ones due for a refresh.

    Pages whose deadline has passed are rechecked weekly instead of every run.
    """
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    today = datetime.now(timezone.utc).date()
    cursor = collection.find({}, {"apply_link": 1, "is_open": 1, "deadline": 1, "last_updated": 1, "_id": 0})
    existing, due = set(), set()
    async for doc in cursor:
        if doc.get("apply_link"):
            existing.add(doc["apply_link"])
            deadline = parse_opportunity_date(doc.get("deadline"), end=True)
            checked = doc.get("last_updated")
            if checked and checked.tzinfo is None:
                checked = checked.replace(tzinfo=timezone.utc)
            long_closed = doc.get("is_open") is False and deadline is not None and deadline < today
            if not (long_closed and checked and checked > week_ago):
                due.add(doc["apply_link"])
    print(f"Found {len(existing)} already-scraped URLs in DB ({len(due)} due for a refresh).")
    return existing, due


# ─────────────────────────── DOMAIN SCORING ──────────────────────

def get_domain_score(url: str) -> int:
    u = url.lower()
    if any(d in u for d in BLACKLISTED_DOMAINS): return 0
    if any(e in u for e in [".gov.in", ".nic.in", ".res.in"]): return 100
    if any(e in u for e in [".ac.in", ".edu.in"]): return 95
    tier2 = ["lfx.linuxfoundation.org", "summerofcode.withgoogle.com",
             "cncf.io", "summerofbitcoin.org", "fossunited.org",
             "jncasr.ac.in", "iitgn.ac.in", "ghc.anitab.org",
             "outreachy.org", "mlh.io", "anitab.org"]
    if any(t in u for t in tier2): return 98
    if any(a in u for a in ["internshala", "unstop", "naukri", "glassdoor", "indeed"]): return 30
    return 50


def is_link_allowed(url: str) -> bool:
    u = url.lower()
    if any(d in u for d in BLACKLISTED_DOMAINS): return False
    if u.endswith((".pdf", ".doc", ".docx", ".zip")): return False
    return True

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    clean = parsed._replace(query="", fragment="")
    return urlunparse(clean).rstrip("/")

def site_of(url: str) -> str:
    """Registered domain: scholarships.reliancefoundation.org -> reliancefoundation.org, iisc.ac.in stays."""
    try:
        parts = urlparse(url).netloc.lower().split(":")[0].split(".")
    except ValueError:  # malformed links such as "https://[..." in page text
        return ""
    keep = 3 if len(parts) > 2 and len(parts[-1]) == 2 and parts[-2] in {"ac", "co", "edu", "gov", "net", "nic", "org", "res"} else 2
    return ".".join(parts[-keep:])

def program_key(name) -> str:
    """Normalize a program name (without years) so the same program on two URLs matches."""
    text = re.sub(r"\b20\d{2}\b", "", str(name or "").lower())
    return re.sub(r"[^a-z0-9]+", "", text)

MONTHS = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?"
DATE_HINTS = re.compile(
    rf"deadline|apply by|last date|timeline|important dates|applications? (open|close)|\b20\d{{2}}\b"
    rf"|\b\d{{1,2}}(?:st|nd|rd|th)?\s+{MONTHS}|\b{MONTHS}\s+\d{{1,2}}\b",
    re.I,
)
DATE_LINK_HINTS = re.compile(r"timeline|dates|deadline|schedule|how to apply|apply|faq|eligib", re.I)
# Links found on pages are only worth a Groq call when the URL looks like an opportunity.
LEAD_HINTS = "fellow|intern|scholar|mentor|summer|winter|research|program|apply|student"

def page_content(result, limit: int = 5000) -> str:
    """Main page content without menus, plus any lines that mention dates further down."""
    raw = str(result.markdown)
    if len(raw) <= limit:
        return raw
    head = (getattr(result.markdown, "fit_markdown", "") or raw)[: limit // 2]
    dated = [line for line in raw.splitlines() if DATE_HINTS.search(line) and line not in head]
    return (head + "\n" + "\n".join(dated))[:limit]

def is_official(doc: dict) -> bool:
    return get_domain_score(doc["apply_link"]) >= 90 or doc.get("is_official_source") is True

def is_alertable(doc: dict) -> bool:
    """Open, not excluding our students, and with a real deadline (or explicitly rolling)."""
    return doc["is_open"] and doc.get("eligible") is not False and has_known_deadline(doc["deadline"])

def page_rank(doc: dict) -> tuple:
    """Which of two pages about one program to keep: official, then open, then latest deadline, then most trusted."""
    deadline = parse_opportunity_date(doc.get("deadline"), end=True)
    return (is_official(doc), bool(doc.get("is_open")), str(deadline or ""), doc.get("trust_score") or 0)

async def find_twin(doc: dict) -> dict | None:
    """Another saved page about the same program: same name, and the same site or organization."""
    if not doc["program_key"]:
        return None
    org = program_key(doc.get("organization"))
    async for other in collection.find({"program_key": doc["program_key"], "apply_link": {"$ne": doc["apply_link"]}}, {"_id": 0}):
        if site_of(other["apply_link"]) == site_of(doc["apply_link"]) or (org and program_key(other.get("organization")) == org):
            return other
    return None

async def skip_for_a_while(link: str):
    """Pages that aren't opportunities, or repeat a saved one, are not read again for 30 days."""
    if not DRY_RUN:
        await skipped_collection.update_one(
            {"apply_link": link}, {"$set": {"checked_at": datetime.now(timezone.utc)}}, upsert=True
        )

async def generate_queries_with_ai() -> list[dict]:
    print("\nGemini is generating search queries...")
    programs_list = "\n".join(f"- {p}" for p in MUST_HAVE_PROGRAMS)
    current_year = datetime.now(timezone.utc).year

    prompt = f"""You are helping find tech fellowships for Indian CS students in Bangalore.

Today is {datetime.now(timezone.utc).date().isoformat()}. The current cycle is
{datetime.now(timezone.utc).year}-{datetime.now(timezone.utc).year + 1}. Search for
opportunities that are current or upcoming from today onward across all years,
using the dates and cycle stated on each official page.
Do not target expired, archived, or previous-cycle opportunities.

For each program below, generate exactly 3 Google search queries:
1. One targeting the current official application page, not an archived page
2. One targeting a current or upcoming deadline in the current or next cycle
3. One targeting current eligibility and application details for Indian students
Programs:
{programs_list}

Also suggest 15 additional relevant programs
Generate 3 queries each for the additional programs too.

Return ONLY this JSON with no extra text or markdown:
{{
  "must_have": [
    {{"name": "Program Name", "queries": ["query 1", "query 2", "query 3"], "official_domain_hint": "domain.com"}}
  ],
  "additional": [
    {{"name": "Program Name", "queries": ["query 1", "query 2"], "official_domain_hint": "domain.com"}}
  ]
}}"""

    raw = await ask_ai(prompt, max_tokens=3000)
    if not raw:
        print("Gemini unavailable, using fallback queries.")
        return [{"name": p, "queries": [f"{p} {current_year} official application", f"{p} deadline {current_year}"]}
                for p in MUST_HAVE_PROGRAMS]

    data = safe_parse_json(raw)
    if not data or not isinstance(data, dict):
        print("JSON parse failed, using fallback queries.")
        return [{"name": p, "queries": [f"{p} {current_year} official application", f"{p} deadline {current_year}"]}
                for p in MUST_HAVE_PROGRAMS]

    combined = data.get("must_have", []) + data.get("additional", [])
    print(f"Generated queries for {len(combined)} programs.")
    return combined

async def get_weekly_programs() -> list[dict]:
    """Program searches run once a week; stored pages are still refreshed every run."""
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    if await db.query_plans.find_one({"created_at": {"$gte": week_ago}}):
        print("\nProgram searches already ran this week, running discovery searches only.")
        return []
    programs = await generate_queries_with_ai()
    if not DRY_RUN:
        await db.query_plans.insert_one({"created_at": datetime.now(timezone.utc), "programs": programs})
    return programs

async def serper_search(query: str, client: httpx.AsyncClient, tbs: str | None = None) -> list[str]:
    headers = {"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"}
    stats["searches"] += 1
    try:
        plain = re.sub(r"\b(?:site|inurl|intitle|intext|filetype):|[^\w\s.\-]", " ", query)
        for q in (query, plain):
            resp = await client.post(
                "https://google.serper.dev/search",
                json={"q": q, "gl": "in", "num": 20, **({"tbs": tbs} if tbs else {})},
                headers=headers, timeout=15,
            )
            # Serper's free plan rejects "quoted phrases" and operators like site: (paid plans
            # search them as written), so only then retry the query as plain keywords.
            if not (resp.status_code == 400 and "not allowed" in resp.text):
                break
        # A bad key or used-up credits must show up as an error, not as "no results".
        resp.raise_for_status()
        results = resp.json().get("organic", [])
        return [
            r.get("link", "") for r in results
            if is_link_allowed(r.get("link", ""))
        ]

    except Exception as e:
        detail = e.response.text[:200] if isinstance(e, httpx.HTTPStatusError) else e
        print(f"Serper error for {query!r}: {detail}")
        stats["search_errors"] += 1
        return []


async def collect_links(programs: list[dict]) -> list[tuple[int, str]]:
    seen, scored = set(), []
    async with httpx.AsyncClient() as http:
        for prog in programs:
            # Stop early instead of burning through every query when Serper rejects them all;
            # saved pages are still refreshed.
            if stats["searches"] >= 5 and stats["search_errors"] == stats["searches"]:
                print("All Serper searches failed. Check the Serper errors above, SERPER_API_KEY and your credits.")
                break
            print(f"Searching: {prog['name']}")
            for query in prog.get("queries", []):
                for link in await serper_search(query, http, prog.get("tbs")):
                    link = normalize_url(link)
                    if link not in seen:
                        seen.add(link)
                        score = get_domain_score(link)
                        hint = prog.get("official_domain_hint", "")
                        if hint and hint.lower() in link.lower():
                            score = min(score + 15, 100)
                        scored.append((score, link))
                await asyncio.sleep(0.5)
    scored.sort(key=lambda x: x[0], reverse=True)
    print(f"\nCollected {len(scored)} unique links.\n")
    return scored

def deduplicate_by_domain(scored_links: list[tuple[int, str]], max_per_domain: int = 1) -> list[tuple[int, str]]:
    """
    Keep only the top N URLs per domain.
    Prevents 5 links from summerofbitcoin.org, 4 from cncf.io etc.
    """
    domain_count = {}
    deduped = []

    for score, url in scored_links:
        domain = urlparse(url).netloc.replace("www.", "")
        count = domain_count.get(domain, 0)
        if count < max_per_domain:
            deduped.append((score, url))
            domain_count[domain] = count + 1

    print(f"Deduplicated: {len(scored_links)} → {len(deduped)} links (max {max_per_domain} per domain)\n")
    return deduped


from urllib.parse import urlparse

def generate_domain_paths(domain_url):
    base = domain_url.rstrip("/")
    paths = [
        "/internships",
        "/fellowships",
        "/research",
        "/careers",
        "/opportunities",
        "/summer-internship",
        "/students",
    ]
    return [base + p for p in paths]

def generate_dynamic_queries():
    topics = [
        "AI", "machine learning", "cybersecurity",
        "software engineering", "data science",
        "open source"
    ]

    templates = [
        "{} fellowship students",
        "{} internship undergraduate",
        "{} research internship apply",
        "{} student mentorship program"
    ]

    queries = []
    for t in topics:
        for template in templates:
            queries.append(template.format(t))

    return queries

def build_doc(details: dict, link: str, score: int) -> dict:
    application_start = (
        details.get("application_start")
        or details.get("start_date")
        or "Check Website"
    )
    deadline = details.get("deadline") or "Check Website"
    is_open = calculate_is_open(
        application_start,
        deadline,
        details.get("is_open"),
        today=datetime.now(timezone.utc).date(),
    )

    return {
        "name":         details.get("name") or "Unknown Opportunity",
        "organization": details.get("organization"),
        "application_start": application_start,
        "deadline":     deadline,
        "stipend":      details.get("stipend"),
        "eligibility":  details.get("eligibility"),
        "mode":         details.get("mode"),
        "is_open":      is_open,
        "tags":         details.get("tags", []),
        "apply_link":   link,
        "trust_score":  score,
        "last_updated": datetime.now(timezone.utc),
        "program_key":  program_key(details.get("name")),
        "eligible":     details.get("eligible"),
        "is_official_source": details.get("is_official_source"),
    }

async def find_official_source(crawler, run_cfg, doc: dict) -> dict | None:
    """Search for the program's official page and return its details if it confirms the opportunity."""
    query = f"{doc['name']} {doc.get('organization') or ''} official apply"
    async with httpx.AsyncClient() as http:
        found = [normalize_url(u) for u in await serper_search(query, http)]
    candidates = sorted(
        (u for u in dict.fromkeys(found) if u != doc["apply_link"] and get_domain_score(u) >= 50),
        key=get_domain_score,
        reverse=True,
    )
    for url in candidates[:3]:
        try:
            result = await asyncio.wait_for(crawler.arun(url=url, config=run_cfg), timeout=60.0)
        except asyncio.TimeoutError:
            continue
        if not result.success or len(result.markdown) < 300:
            continue
        async with ai_lock:
            details = await ai_extract_details(
                page_content(result), url, expected=f"{doc['name']} ({doc.get('organization')})"
            )
        if details.get("is_opportunity") and details.get("same_program"):
            source = build_doc(details, url, get_domain_score(url))
            if is_official(source):
                return source
    return None

async def find_dates(crawler, run_cfg, details: dict, link: str, markdown: str) -> dict | None:
    """Find a missing deadline on the program's own timeline/apply/FAQ pages, then through search."""
    site = site_of(link)
    program = f"{details.get('name')} ({details.get('organization')})"

    async def dates_from(url: str) -> dict | None:
        try:
            result = await asyncio.wait_for(crawler.arun(url=url, config=run_cfg), timeout=60.0)
        except asyncio.TimeoutError:
            return None
        if not result.success or len(result.markdown) < 300:
            return None
        async with ai_lock:
            found = await ai_extract_details(page_content(result), url, expected=program)
        if found.get("same_program") and has_known_deadline(found.get("deadline")):
            print(f"  Dates for {details.get('name')} found on {url}")
            return found
        return None

    # 1. Links on the same site such as "Timeline", "Important dates", "How to apply", "FAQ".
    same_site = [
        normalize_url(url) for text, url in re.findall(r"\[([^\]]{2,80})\]\((https?://[^)\s]+)\)", markdown)
        if DATE_LINK_HINTS.search(text) and site_of(url) == site
    ]
    tried = {normalize_url(link)}
    for url in [u for u in dict.fromkeys(same_site) if u not in tried][:2]:
        tried.add(url)
        if found := await dates_from(url):
            return found

    # 2. Search for the deadline and read same-site or official results only.
    query = f"{details.get('name')} {details.get('organization') or ''} application deadline {datetime.now(timezone.utc).year}"
    async with httpx.AsyncClient() as http:
        results = [normalize_url(u) for u in await serper_search(query, http)]
    trusted = [u for u in dict.fromkeys(results) if u not in tried and (site_of(u) == site or get_domain_score(u) >= 90)]
    for url in trusted[:2]:
        if found := await dates_from(url):
            return found
    return None

async def send_verified_alert(crawler, run_cfg, doc: dict, event: str):
    """Send a WhatsApp alert only for an open opportunity confirmed by an official page."""
    if not DRY_RUN and not (WHATSAPP_ALERT_URL and WHATSAPP_ALERT_SECRET):
        return  # alerts are off, so skip the verification searches
    link = doc["apply_link"]
    if doc.get("eligible") is False:
        print(f"Not alerting, our students are not eligible: {doc['name']}")
        return
    recent = datetime.now(timezone.utc) - timedelta(days=60)
    if event == "new" and doc["program_key"] and await collection.find_one({
        "program_key": doc["program_key"],
        "apply_link": {"$ne": link},
        "alerted_at": {"$gte": recent},
    }):
        print(f"Not alerting, already announced from another page: {doc['name']}")
        return
    source = doc if is_official(doc) else await find_official_source(crawler, run_cfg, doc)
    if not source or not is_alertable(source):
        stats["unverified"] += 1
        print(f"Not alerting, no open official page with a deadline: {doc['name']}")
        return

    now = datetime.now(timezone.utc)
    if DRY_RUN:
        stats["alerts"] += 1
        print(f"[DRY RUN] Would send WhatsApp alert ({event}): {source['name']} -> {source['apply_link']}")
        return
    if await send_whatsapp_alert(
        source,
        event=event,
        idempotency_key=make_idempotency_key(event, source["apply_link"], now),
        url=WHATSAPP_ALERT_URL,
        secret=WHATSAPP_ALERT_SECRET,
    ):
        stats["alerts"] += 1
        await collection.update_one(
            {"apply_link": link},
            {"$set": {"alerted_at": now, "verified_source": source["apply_link"]}},
        )
        if source is not doc:
            # Save the official page too, so finding it later does not announce the program again.
            await collection.update_one(
                {"apply_link": source["apply_link"]},
                {"$set": {**source, "alerted_at": now}, "$setOnInsert": {"first_seen": now}},
                upsert=True,
            )

async def process_link(crawler, run_cfg, link: str, score: int, semaphore: asyncio.Semaphore):
    async with semaphore:
        if stats["ai_daily_limit"]:
            return
        try:
            result = await asyncio.wait_for(
                crawler.arun(url=link, config=run_cfg), timeout=60.0
            )
            if not result.success or len(result.markdown) < 300:
                return
            stats["crawled"] += 1

            links = [l.rstrip(".,;") for l in re.findall(r'https?://[^\s)"<>*\]]+', result.markdown)]
            # Links to other sites are leads for later runs, including links on listing pages.
            page_site = site_of(link)
            outbound = [l for l in links if site_of(l) not in ("", page_site)]

            for l in outbound[:20]:

                l = normalize_url(l)

                if not is_link_allowed(l):
                    continue

                if get_domain_score(l) < 50 or DRY_RUN:
                    continue

                await discovered_collection.update_one(
                    {"apply_link": l},
                    {"$setOnInsert": {
                    "name": "Discovered Page",
                    "apply_link": l,
                    "trust_score": score - 10,
                    "last_updated": datetime.now(timezone.utc)
                }},
                upsert=True
                )
            if score < 80 and result.markdown.count("](") > 80:
                print(f"Skipping aggregator: {link}")
                return

            text = page_content(result)
            content_hash = hashlib.sha256(text.encode()).hexdigest()
            ai_errors = stats["ai_errors"]
            previous = await collection.find_one({"apply_link": link}, {"_id": 0})
            if previous and previous.get("content_hash") == content_hash:
                # Page unchanged: reuse the last extraction; its dates are re-checked against today below.
                details = previous
            else:
                async with ai_lock:
                    details = await ai_extract_details(text, link)

                if not details:
                    print(f"AI extraction failed: {link}")
                    return
                if not details.get("is_opportunity"):
                    print(f"Skipping non-opportunity page: {link}")
                    if previous and not previous.get("is_manual"):
                        # Saved by an earlier run but not an opportunity (blog, listing, guide): remove it from the site.
                        stats["deleted"] += 1
                        if DRY_RUN:
                            print(f"[DRY RUN] Would delete saved page: {link}")
                        else:
                            await collection.delete_one({"apply_link": link})
                            print(f"Deleted saved page that is not an opportunity: {link}")
                    await skip_for_a_while(link)
                    return

                if details.get("eligible") is not False and not has_known_deadline(details.get("deadline")):
                    dates = await find_dates(crawler, run_cfg, details, link, str(result.markdown))
                    if dates:
                        stats["dates_found"] += 1
                        details = {**details, **{k: dates.get(k) for k in ("application_start", "deadline", "is_open")}}

                await asyncio.sleep(1)

            doc = build_doc(details, link, score)
            # If a Groq call failed (e.g. while looking for dates), read the page again next run.
            doc["content_hash"] = content_hash if stats["ai_errors"] == ai_errors else None
            is_new = previous is None
            # One record per program: keep the better page and drop the other.
            carried = {}
            twin = None if (previous or {}).get("is_manual") else await find_twin(doc)
            if twin and (twin.get("is_manual") or page_rank(twin) >= page_rank(doc)):
                stats["duplicates"] += 1
                print(f"Already saved from {twin['apply_link']}: {doc['name']}")
                await skip_for_a_while(link)
                if previous and not DRY_RUN:
                    await collection.delete_one({"apply_link": link})
                    if previous.get("alerted_at"):
                        await collection.update_one(
                            {"apply_link": twin["apply_link"]}, {"$max": {"alerted_at": previous["alerted_at"]}}
                        )
                return
            if twin:
                stats["duplicates"] += 1
                print(f"Replacing {twin['apply_link']} with a better page for {doc['name']}")
                if not DRY_RUN:
                    await collection.delete_one({"apply_link": twin["apply_link"]})
                await skip_for_a_while(twin["apply_link"])
                if twin.get("alerted_at") and not (previous or {}).get("alerted_at"):
                    carried["alerted_at"] = twin["alerted_at"]  # already announced from the old page
                previous, is_new = previous or twin, False
            if not DRY_RUN:
                await collection.update_one(
                    {"apply_link": link},
                    {"$set": {**doc, **carried}, "$setOnInsert": {"first_seen": doc["last_updated"]}},
                    upsert=True,
                )
            stats["saved"] += 1
            alert_event = classify_opportunity_event(
                previous,
                is_new=is_new,
                is_open=doc["is_open"],
            )

            if is_new:
                stats["new"] += 1
                print(f"New opportunity! Sending Discord notification...")
                if not DRY_RUN:
                    await send_discord_notification(doc)
            if alert_event:
                await send_verified_alert(crawler, run_cfg, doc, alert_event)

            print(f"Saved: {doc['name']}  |  Deadline: {doc['deadline']}")

        except asyncio.TimeoutError:
            print(f"Timeout: {link}")
        except Exception as e:
            print(f"Error ({link}): {e}")

async def ai_extract_details(page_text: str, url: str, expected: str | None = None) -> dict:

    profile = (
        f"a {STUDENT_PROFILE['education']} student in {STUDENT_PROFILE['location']} "
        f"interested in {', '.join(STUDENT_PROFILE['domains'])}"
    )
    same_program = (
        f'\nAlso return "same_program": true only if this page is about the same program as: {expected}.'
        if expected else ""
    )
    prompt = f"""
Extract opportunity data from this webpage.

Today is {datetime.now(timezone.utc).date().isoformat()}. The current cycle is
{datetime.now(timezone.utc).year}-{datetime.now(timezone.utc).year + 1}.
Use the actual current page content, not search-result snippets, examples, or
old cached details. 
If this is a valid opportunity page but its application window has already closed, still return the full opportunity details with "is_open": false so
the database can be updated. Return {{ "is_opportunity": false }} only when the page is unrelated to an opportunity.

If the page is NOT about a fellowship, internship,
research program, mentorship, or scholarship,
return:

{{ "is_opportunity": false }}

Otherwise return:

{{
  "is_opportunity": true,
  "name": "Full program name",
  "organization": "Sponsoring organization",
  "application_start": "YYYY-MM-DD or Check Website or Rolling",
  "deadline": "YYYY-MM-DD or Check Website or Rolling",
  "stipend": "Amount or Unpaid or Not Specified",
  "eligibility": "1-2 sentence summary",
  "mode": "Remote or In-Person or Hybrid",
  "is_open": true,
  "tags": ["tag1", "tag2"],
  "eligible": true,
  "is_official_source": true
}}
Extract the application opening date and deadline from the page. Set
"is_open" to true only when today is on or after application_start and on or
before deadline. If either date is unavailable, use the page's explicit status.
Set "eligible" to false only if the eligibility rules clearly exclude {profile}.
Set "is_official_source" to true only if this page is published by the organization
that runs the program, not a news site, blog, job board or listing site.{same_program}
Return only JSON.

URL:
{url}

Content:
{page_text[:5000]}
"""

    raw = await ask_ai(prompt, max_tokens=900)

    if not raw:
        return {}

    result = safe_parse_json(raw)

    if not isinstance(result, dict):
        return {}

    return result

async def ensure_indexes():
    await collection.create_index("apply_link", unique=True)
    await collection.create_index("last_updated")
    await collection.create_index("program_key")
    await skipped_collection.create_index("checked_at", expireAfterSeconds=30 * 24 * 3600)

async def ping_mongo():
    await mongo_client.admin.command("ping")

async def main():
    await ping_mongo()
    if not DRY_RUN:
        await ensure_indexes()
    print("=" * 60)
    print("  FELLOWSHIP TRACKER — AI MODE" + ("  [DRY RUN: no DB writes or alerts]" if DRY_RUN else ""))
    print(f"  Model: {GROQ_MODEL}")
    if not (WHATSAPP_ALERT_URL and WHATSAPP_ALERT_SECRET):
        print("  WhatsApp alerts: off (WHATSAPP_ALERT_URL / WHATSAPP_ALERT_SECRET not set)")
    print("=" * 60)

    programs     = await get_weekly_programs()
    year         = datetime.now(timezone.utc).year

    # Generic searches name this year and next and only return pages from the past week, so they
    # find newly posted programs. Each runs every 4th day (inside its one-week window) to save credits.
    generic = DISCOVERY_QUERIES + generate_dynamic_queries()
    for q in generic[datetime.now(timezone.utc).toordinal() % 4::4]:
        programs.append({
        "name": "Discovery",
        "queries": [f"{q} {year} OR {year + 1}"],
        "official_domain_hint": "",
        "tbs": "qdr:w",
    })
    print("\n Running web searches...\n")
    scored_links = await collect_links(programs)

    # Links found on crawled pages in earlier runs are leads too.
    # They rank below search results.
    leads = [
        d["apply_link"]
        async for d in discovered_collection.find(
            {"checked_at": {"$exists": False}, "apply_link": {"$regex": LEAD_HINTS, "$options": "i"}},
            {"apply_link": 1, "_id": 0},
        ).sort("last_updated", 1).limit(100)
    ]
    scored_links += [(get_domain_score(url) - 10, url) for url in leads]
    if leads and not DRY_RUN:
        await discovered_collection.update_many(
            {"apply_link": {"$in": leads}}, {"$set": {"checked_at": datetime.now(timezone.utc)}}
        )

    if not scored_links:
        print(" No links found. Check SERPER_API_KEY in .env")

    existing_urls, due_urls = await get_existing_urls()
    # Pages found in the last 30 days not to be opportunities (or to repeat a saved one) aren't read again.
    seen = existing_urls | {d["apply_link"] async for d in skipped_collection.find({}, {"apply_link": 1, "_id": 0})}
    # Cap each domain among new links only, so already-saved pages don't use up its slots.
    scored_links  = deduplicate_by_domain(
        [(score, url) for score, url in scored_links if url not in seen], max_per_domain=2
    ) + [(score, url) for score, url in scored_links if url in existing_urls]
    for domain in DISCOVERY_DOMAINS:
        for path in generate_domain_paths(domain):
            scored_links.append((85, normalize_url(path)))

    score_by_url = {}
    for score, url in scored_links:
        score_by_url[url] = max(score_by_url.get(url, 0), score)
    scored_links = [(score, url) for url, score in score_by_url.items()]
    scored_links.sort(key=lambda item: item[0], reverse=True)
    new_links = [(score, url) for score, url in scored_links if url not in seen]
    refresh_links = [
        (
            max(
                80,
                score_by_url.get(url, get_domain_score(url)),
            ),
            url,
        )
        for url in due_urls
    ]
    print(
        f" {len(new_links)} new links and {len(refresh_links)} existing links available for processing\n"
    )

    # Revisit stored links so their application window and database status
    # are refreshed even when search no longer returns the old URL. They go first:
    # unchanged pages need no Groq call, so the daily limit is spent on new pages after them.
    final_pairs = refresh_links + sorted(new_links, reverse=True)[:150]
    final_urls = [url for _, url in final_pairs]
    score_map  = {url: score for score, url in final_pairs}

    print(f"\n Crawling {len(final_urls)} pages...\n")
    semaphore = asyncio.Semaphore(3)
    run_cfg   = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        exclude_all_images=True,
        page_timeout=60000,
        wait_for="body",
        delay_before_return_html=2.0,
        # Adds result.markdown.fit_markdown: the main content without menus and footers.
        markdown_generator=DefaultMarkdownGenerator(content_filter=PruningContentFilter()),
    )

    async with AsyncWebCrawler() as crawler:
        tasks = [
            process_link(crawler, run_cfg, url, score_map.get(url, 50), semaphore)
            for url in final_urls
        ]
        await asyncio.gather(*tasks)

    print("\n Done! Database updated.")
    print(f" Summary: {dict(stats)}")
    if stats["ai_calls"] and stats["ai_errors"] == stats["ai_calls"] and not stats["ai_daily_limit"]:
        raise SystemExit("All Groq calls failed. Check GROQ_API_KEY and GROQ_MODEL.")


if __name__ == "__main__":
    asyncio.run(main())