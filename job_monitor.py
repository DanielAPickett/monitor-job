#!/usr/bin/env python3
"""
GIS Job Monitor
===============
Polls job sources, finds NEW remote GIS openings that fit your profile, and
pushes them to your phone (ntfy) and/or email the instant they appear.

Design notes
------------
- This monitors *structured, bot-friendly* sources: ATS APIs (Greenhouse, Lever),
  the official USAJOBS API, and RSS feeds. It deliberately does NOT scrape Indeed,
  LinkedIn, Glassdoor, or ZipRecruiter: those block bots, require login, and their
  terms forbid scraping, so anything built on them breaks within days. The ATS
  APIs below are the same feeds those aggregators pull from, just at the source.
- State (which jobs you've already been told about) lives in seen_jobs.json.
  In GitHub Actions, the workflow commits that file back to the repo each run.

Run locally:
    python job_monitor.py            # one polling cycle
    python job_monitor.py --test     # offline self-test with fixtures (no network)
    python job_monitor.py --dry-run  # fetch + score but don't send notifications
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import smtplib
import sys
import time
from dataclasses import dataclass, field, asdict
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import requests
import yaml

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "config.yaml"
STATE_PATH = HERE / "seen_jobs.json"
STATE_RETENTION_DAYS = 60          # forget jobs we haven't seen in this long
MAX_NOTIFICATIONS_PER_RUN = 15     # spam guard if a board dumps a batch
HTTP_TIMEOUT = 25
USER_AGENT = "gis-job-monitor/1.0 (+personal job alert)"

REMOTE_WORDS = ("remote", "anywhere", "telework", "work from home",
                "work-from-home", "wfh", "distributed", "virtual", "us-remote")


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #
@dataclass
class Job:
    id: str
    title: str
    company: str
    location: str
    url: str
    source: str
    posted: str = ""
    remote: bool = False
    snippet: str = ""
    score: int = 0
    reasons: list[str] = field(default_factory=list)

    def blob(self) -> str:
        return f"{self.title} \n {self.location} \n {self.snippet}".lower()


def make_id(source: str, native: str) -> str:
    return f"{source}:{hashlib.sha1(native.encode('utf-8')).hexdigest()[:16]}"


def looks_remote(*texts: str) -> bool:
    t = " ".join(texts).lower()
    return any(w in t for w in REMOTE_WORDS)


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = re.sub(r"&[a-z]+;", " ", raw)
    return re.sub(r"\s+", " ", raw).strip()


# --------------------------------------------------------------------------- #
# Source adapters  (each returns list[Job], never raises)
# --------------------------------------------------------------------------- #
def fetch_greenhouse(src: dict) -> list[Job]:
    """Greenhouse public boards API. slug = the company's board name."""
    slug = src["slug"]
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    data = _get_json(url)
    jobs = []
    for j in (data or {}).get("jobs", []):
        loc = (j.get("location") or {}).get("name", "") or ""
        snippet = clean_html(j.get("content", ""))[:600]
        jobs.append(Job(
            id=make_id("greenhouse-" + slug, str(j.get("id"))),
            title=j.get("title", "").strip(),
            company=src.get("name", slug),
            location=loc,
            url=j.get("absolute_url", ""),
            source=f"Greenhouse/{src.get('name', slug)}",
            posted=j.get("updated_at", "") or j.get("first_published", ""),
            remote=looks_remote(loc, j.get("title", "")),
            snippet=snippet,
        ))
    return jobs


def fetch_lever(src: dict) -> list[Job]:
    """Lever public postings API. slug = the company handle."""
    slug = src["slug"]
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    data = _get_json(url)
    jobs = []
    for j in (data or []):
        cats = j.get("categories", {}) or {}
        loc = cats.get("location", "") or ""
        wt = (cats.get("commitment", "") or "") + " " + (j.get("workplaceType", "") or "")
        snippet = clean_html(j.get("descriptionPlain", "") or j.get("description", ""))[:600]
        jobs.append(Job(
            id=make_id("lever-" + slug, j.get("id", j.get("hostedUrl", ""))),
            title=j.get("text", "").strip(),
            company=src.get("name", slug),
            location=loc,
            url=j.get("hostedUrl", ""),
            source=f"Lever/{src.get('name', slug)}",
            posted=_ms_to_iso(j.get("createdAt")),
            remote=looks_remote(loc, wt, j.get("text", "")),
            snippet=snippet,
        ))
    return jobs


def fetch_usajobs(src: dict, key: str, email: str) -> list[Job]:
    """Official USAJOBS Search API. Needs a free API key (developer.usajobs.gov)."""
    if not key or not email:
        print("  ! USAJOBS skipped: set USAJOBS_KEY and USAJOBS_EMAIL")
        return []
    headers = {"Host": "data.usajobs.gov", "User-Agent": email, "Authorization-Key": key}
    params = {
        "Keyword": src.get("keyword", "GIS"),
        "ResultsPerPage": "50",
    }
    # 0150 = Geography, 1370 = Cartography (optional narrowing)
    if src.get("job_category"):
        params["JobCategoryCode"] = src["job_category"]
    try:
        r = requests.get("https://data.usajobs.gov/api/search",
                         headers=headers, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        items = r.json()["SearchResult"]["SearchResultItems"]
    except Exception as e:  # noqa: BLE001
        print(f"  ! USAJOBS error: {e}")
        return []
    jobs = []
    for it in items:
        d = it.get("MatchedObjectDescriptor", {})
        locs = ", ".join(sorted({l.get("LocationName", "")
                                 for l in d.get("PositionLocation", [])}))[:120]
        remote_flag = str(d.get("PositionRemote", "")).lower()
        snippet = clean_html((d.get("QualificationSummary") or
                              d.get("UserArea", {}).get("Details", {}).get("JobSummary", "")))[:600]
        jobs.append(Job(
            id=make_id("usajobs", d.get("PositionID", d.get("MatchedObjectId", ""))),
            title=d.get("PositionTitle", "").strip(),
            company="USAJOBS / " + d.get("OrganizationName", "Federal"),
            location=locs,
            url=d.get("PositionURI", ""),
            source="USAJOBS",
            posted=d.get("PublicationStartDate", ""),
            remote=("remote" in remote_flag or "yes" in remote_flag
                    or looks_remote(locs, d.get("PositionTitle", ""))),
            snippet=snippet,
        ))
    return jobs


def fetch_rss(src: dict) -> list[Job]:
    """Generic RSS/Atom adapter (ApplyToJob/JazzHR, GovernmentJobs, GIS boards)."""
    try:
        import feedparser  # imported lazily so the module loads without it
    except ImportError:
        print("  ! RSS skipped: `pip install feedparser`")
        return []
    feed = feedparser.parse(src["url"], request_headers={"User-Agent": USER_AGENT})
    jobs = []
    for e in feed.entries:
        title = getattr(e, "title", "").strip()
        loc = getattr(e, "location", "") or getattr(e, "where", "") or ""
        link = getattr(e, "link", "")
        snippet = clean_html(getattr(e, "summary", "") or getattr(e, "description", ""))[:600]
        jobs.append(Job(
            id=make_id("rss-" + src.get("name", "feed"), getattr(e, "id", link) or link),
            title=title,
            company=src.get("name", "RSS"),
            location=loc,
            url=link,
            source="RSS/" + src.get("name", "feed"),
            posted=getattr(e, "published", "") or getattr(e, "updated", ""),
            remote=looks_remote(title, loc, snippet),
            snippet=snippet,
        ))
    return jobs


def fetch_workday(src: dict) -> list[Job]:
    """Workday CXS endpoint (advanced/fragile). Provide full 'endpoint' + optional 'search'."""
    endpoint = src["endpoint"]  # e.g. https://x.wd5.myworkdayjobs.com/wday/cxs/x/Careers/jobs
    body = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": src.get("search", "GIS")}
    try:
        r = requests.post(endpoint, json=body,
                          headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                          timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        postings = r.json().get("jobPostings", [])
    except Exception as e:  # noqa: BLE001
        print(f"  ! Workday error ({src.get('name')}): {e}")
        return []
    base = re.sub(r"/wday/cxs/.*$", "", endpoint)
    jobs = []
    for p in postings:
        path = p.get("externalPath", "")
        loc = p.get("locationsText", "") or ""
        title = p.get("title", "").strip()
        jobs.append(Job(
            id=make_id("workday-" + src.get("name", ""), path or title),
            title=title,
            company=src.get("name", "Workday"),
            location=loc,
            url=base + path if path else endpoint,
            source="Workday/" + src.get("name", ""),
            posted=p.get("postedOn", ""),
            remote=looks_remote(title, loc),
            snippet="",
        ))
    return jobs


ADAPTERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "usajobs": fetch_usajobs,
    "rss": fetch_rss,
    "workday": fetch_workday,
}


def _get_json(url: str) -> Any:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                         timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:  # noqa: BLE001
        print(f"  ! fetch error {url.split('?')[0]}: {e}")
        return None


def _ms_to_iso(ms: Any) -> str:
    try:
        return dt.datetime.utcfromtimestamp(int(ms) / 1000).date().isoformat()
    except Exception:  # noqa: BLE001
        return ""


# --------------------------------------------------------------------------- #
# Relevance scoring  -- tuned to a GIS Technician/Analyst profile
# --------------------------------------------------------------------------- #
def score_job(job: Job, f: dict) -> Job:
    title = job.title.lower()
    blob = job.blob()
    reasons: list[str] = []
    score = 0

    inc = [k.lower() for k in f.get("include_keywords", [])]
    if not any(k in blob for k in inc):
        job.score, job.reasons = -99, ["no GIS keyword"]
        return job
    if any(k in title for k in inc):
        score += 3
        reasons.append("GIS in title")
    else:
        score += 1

    for k in f.get("boost_keywords", []):
        if k.lower() in blob:
            score += 1
            reasons.append(k)

    if any(k.lower() in title for k in f.get("good_titles", [])):
        score += 2
        reasons.append("fit level")

    if any(k.lower() in title for k in f.get("senior_titles", [])):
        score -= 2
        reasons.append("senior (deprioritized)")

    if job.remote:
        score += 1
        reasons.append("remote")

    job.score = score
    job.reasons = reasons
    return job


def passes(job: Job, f: dict) -> bool:
    """Remote jobs from anywhere pass; non-remote jobs pass only if their
    location matches an entry in filters.local_areas (e.g. Orange County)."""
    if job.score < f.get("min_score", 2):
        return False
    if job.remote or not f.get("remote_only", True):
        return True
    loc = job.location.lower()
    return any(a.lower() in loc for a in f.get("local_areas", []))


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #
def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:  # noqa: BLE001
            return {}
    return {}


def save_state(state: dict) -> None:
    cutoff = (dt.datetime.utcnow() - dt.timedelta(days=STATE_RETENTION_DAYS)).isoformat()
    pruned = {k: v for k, v in state.items() if v.get("last_seen", "") >= cutoff}
    STATE_PATH.write_text(json.dumps(pruned, indent=2, sort_keys=True))


# --------------------------------------------------------------------------- #
# Notifications
# --------------------------------------------------------------------------- #
def notify_ntfy(job: Job, topic: str, priority: str = "high") -> None:
    body = f"{job.company} — {job.location or 'location n/a'}\nwhy: {', '.join(job.reasons[:5])}"
    try:
        requests.post(
            f"https://ntfy.sh/{topic}",
            data=body.encode("utf-8"),
            headers={
                "Title": f"GIS: {job.title}"[:120],
                "Priority": priority,
                "Tags": "briefcase",
                "Click": job.url or "https://ntfy.sh",
            },
            timeout=HTTP_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001
        print(f"  ! ntfy error: {e}")


def notify_ntfy_summary(topic: str, msg: str, priority: str = "default") -> None:
    try:
        requests.post(f"https://ntfy.sh/{topic}", data=msg.encode("utf-8"),
                      headers={"Title": "GIS Job Monitor", "Priority": priority,
                               "Tags": "satellite"},
                      timeout=HTTP_TIMEOUT)
    except Exception as e:  # noqa: BLE001
        print(f"  ! ntfy error: {e}")


def notify_email(jobs: list[Job], cfg: dict) -> None:
    n = cfg.get("notify", {})
    host = os.getenv("SMTP_HOST")
    user = os.getenv("SMTP_USER")
    pw = os.getenv("SMTP_PASS")
    to = n.get("email_to")
    if not (host and user and pw and to):
        return
    lines = ["New GIS openings matching your profile:\n"]
    for j in jobs:
        lines.append(f"• {j.title}  ({j.company})")
        lines.append(f"  {j.location or 'location n/a'}  | score {j.score} | {', '.join(j.reasons[:4])}")
        lines.append(f"  {j.url}\n")
    msg = MIMEText("\n".join(lines))
    msg["Subject"] = f"[GIS Jobs] {len(jobs)} new match(es)"
    msg["From"] = user
    msg["To"] = to
    try:
        port = int(os.getenv("SMTP_PORT", "587"))
        with smtplib.SMTP(host, port, timeout=HTTP_TIMEOUT) as s:
            s.starttls()
            s.login(user, pw)
            s.send_message(msg)
        print(f"  > emailed {len(jobs)} job(s) to {to}")
    except Exception as e:  # noqa: BLE001
        print(f"  ! email error: {e}")


# --------------------------------------------------------------------------- #
# Core cycle
# --------------------------------------------------------------------------- #
def gather(cfg: dict) -> list[Job]:
    jobs: list[Job] = []
    key, email = os.getenv("USAJOBS_KEY", ""), os.getenv("USAJOBS_EMAIL", "")
    for src in cfg.get("sources", []):
        if src.get("enabled", True) is False:
            continue
        fn = ADAPTERS.get(src.get("type"))
        if not fn:
            print(f"  ! unknown source type: {src.get('type')}")
            continue
        print(f"- {src.get('name', src.get('type'))} ({src.get('type')})")
        try:
            if src["type"] == "usajobs":
                found = fn(src, key, email)
            else:
                found = fn(src)
            print(f"    {len(found)} postings")
            jobs.extend(found)
        except Exception as e:  # noqa: BLE001
            print(f"    ! adapter crashed: {e}")
        time.sleep(1)  # be polite between sources
    return jobs


def run(cfg: dict, dry_run: bool = False) -> None:
    f = cfg.get("filters", {})
    state = load_state()
    first_run = len(state) == 0
    now = dt.datetime.utcnow().isoformat()

    raw = gather(cfg)
    # de-dup by id across sources
    uniq: dict[str, Job] = {}
    for j in raw:
        if j.id not in uniq:
            uniq[j.id] = j
    scored = [score_job(j, f) for j in uniq.values()]
    matches = [j for j in scored if passes(j, f)]
    matches.sort(key=lambda j: j.score, reverse=True)

    new_matches = [j for j in matches if j.id not in state]

    print(f"\nseen {len(uniq)} unique postings | {len(matches)} match filter | "
          f"{len(new_matches)} new")

    topic = os.getenv("NTFY_TOPIC") or cfg.get("notify", {}).get("ntfy_topic", "")
    prio = cfg.get("notify", {}).get("ntfy_priority", "high")

    if dry_run:
        for j in new_matches[:25]:
            print(f"  [{j.score}] {j.title} — {j.company} — {j.url}")
    elif first_run:
        # Don't blast every existing posting on the very first run.
        if topic:
            notify_ntfy_summary(topic,
                                f"Monitor live. Tracking {len(matches)} current GIS "
                                f"matches; you'll get a ping when new ones appear.")
        print("  (first run: seeded state, sent one summary ping)")
    else:
        to_send = new_matches[:MAX_NOTIFICATIONS_PER_RUN]
        for j in to_send:
            if topic:
                notify_ntfy(j, topic, prio)
            print(f"  > notified: [{j.score}] {j.title} — {j.company}")
        if cfg.get("notify", {}).get("email_enabled") and to_send:
            notify_email(to_send, cfg)
        if len(new_matches) > MAX_NOTIFICATIONS_PER_RUN and topic:
            notify_ntfy_summary(topic,
                                f"...and {len(new_matches) - MAX_NOTIFICATIONS_PER_RUN} "
                                f"more new matches (capped to avoid spam).")

    # update state: remember every posting we saw this cycle
    for j in uniq.values():
        rec = state.get(j.id, {})
        rec.setdefault("first_seen", now)
        rec.update({"last_seen": now, "title": j.title,
                    "company": j.company, "url": j.url, "score": j.score})
        state[j.id] = rec
    if not dry_run:
        save_state(state)


# --------------------------------------------------------------------------- #
# Offline self-test (no network)
# --------------------------------------------------------------------------- #
def selftest() -> None:
    print("Running offline self-test with fixtures...\n")
    f = {
        "remote_only": True,
        "include_keywords": ["gis", "geospatial", "arcgis", "esri", "cartograph",
                             "spatial", "geographic", "mapping"],
        "boost_keywords": ["arcpy", "python", "experience builder", "arcade",
                            "survey123", "arcgis online", "agol", "arcgis pro",
                            "fme", "dashboard", "web map", "sql"],
        "good_titles": ["technician", "analyst", "specialist", "associate",
                        "junior", "entry", "developer", "cartographer"],
        "senior_titles": ["senior", "lead", "principal", "manager", "director",
                          "architect", "iii", "iv", "supervisor"],
        "min_score": 2,
        "local_areas": ["orange county", "irvine", "santa ana", "anaheim"],
    }
    fixtures = [
        Job(id="t1", title="GIS Analyst I", company="ACME", location="Remote, US",
            url="http://x/1", source="t",
            snippet="ArcGIS Pro, ArcPy and Python automation, AGOL dashboards.",
            remote=True),
        Job(id="t2", title="Senior GIS Architect", company="ACME", location="Remote",
            url="http://x/2", source="t",
            snippet="Lead enterprise ArcGIS Enterprise deployments.", remote=True),
        Job(id="t3", title="Barista", company="Cafe", location="Remote",
            url="http://x/3", source="t", snippet="Make coffee.", remote=True),
        Job(id="t4", title="GIS Technician", company="CityCo", location="Torrance, CA",
            url="http://x/4", source="t",
            snippet="Survey123 and Experience Builder, ArcGIS Pro.", remote=False),
        Job(id="t5", title="Geospatial Developer", company="MapCo", location="Anywhere",
            url="http://x/5", source="t",
            snippet="Build web maps with Arcade and the ArcGIS JavaScript API; SQL.",
            remote=True),
        Job(id="t6", title="GIS Analyst I", company="County of Orange",
            location="Santa Ana, CA", url="http://x/6", source="t",
            snippet="ArcGIS Pro, parcel data, dashboards.", remote=False),
    ]
    for j in fixtures:
        score_job(j, f)
        verdict = "MATCH " if passes(j, f) else "skip  "
        print(f"  {verdict} score={j.score:>3}  {j.title:<24} remote={j.remote}  "
              f"[{', '.join(j.reasons[:4])}]")

    assert passes(fixtures[0], f), "GIS Analyst I should match"
    assert not passes(fixtures[2], f), "Barista should not match"
    assert not passes(fixtures[3], f), "non-remote outside local_areas should be filtered"
    assert passes(fixtures[4], f), "Geospatial Developer should match"
    assert passes(fixtures[5], f), "OC in-person job should pass via local_areas"
    # senior role: matches keyword but is deprioritized; still surfaces if score high enough
    print("\n  notification preview:")
    print(f"    Title: GIS: {fixtures[0].title}")
    print(f"    Body : {fixtures[0].company} — {fixtures[0].location}\n"
          f"           why: {', '.join(fixtures[0].reasons[:5])}")
    print("\nAll assertions passed. Core logic OK.")


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="GIS remote-job monitor")
    ap.add_argument("--test", action="store_true", help="offline self-test, no network")
    ap.add_argument("--dry-run", action="store_true", help="fetch + score, no notifications")
    args = ap.parse_args()

    if args.test:
        selftest()
        return

    if not CONFIG_PATH.exists():
        sys.exit(f"Missing config: {CONFIG_PATH}")
    cfg = yaml.safe_load(CONFIG_PATH.read_text())
    run(cfg, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
