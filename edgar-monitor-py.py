import time
import json
import os
import requests
from datetime import datetime, timedelta

# ── Configuration ────────────────────────────────────────────────────────────
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL", "60"))
DAYS_BACK_ON_START = int(os.environ.get("DAYS_BACK_ON_START", "1"))

COMPANIES = [
    {"name": "Petrobras",       "query": "Petrobras",       "cik": "1119689"},
    {"name": "Vale",            "query": "Vale S.A.",        "cik": "1137774"},
    {"name": "Itaú Unibanco",  "query": "Itau Unibanco",    "cik": "1125699"},
    {"name": "Bradesco",        "query": "Bradesco",         "cik": "906163"},
    {"name": "Embraer",         "query": "Embraer",          "cik": "1124847"},
    {"name": "Ambev",           "query": "Ambev",            "cik": "1075686"},
    {"name": "Banco do Brasil", "query": "Banco do Brasil",  "cik": "1453015"},
    {"name": "Gerdau",          "query": "Gerdau",           "cik": "1114700"},
    # Add more companies here:
    # {"name": "Nubank",        "query": "Nu Holdings",      "cik": "1851627"},
    # {"name": "XP Inc",        "query": "XP Inc",           "cik": "1816937"},
]

HEADERS = {"User-Agent": "EdgarMonitor rodrigo.barneschi@nubank.com.br"}

FORM_EMOJIS = {
    "20-F": "📋", "6-K": "📣", "F-3": "📄",
    "F-1": "🚀", "SC 13G": "🔍", "SC 13D": "🔍",
    "424B": "💰",
}

def get_emoji(form_type):
    for k, v in FORM_EMOJIS.items():
        if form_type and form_type.startswith(k):
            return v
    return "📎"

def pad_cik(cik):
    return str(cik).zfill(10)

# ── CIK-based fetch ──────────────────────────────────────────────────────────
def fetch_by_cik(company, date_from):
    url = f"https://data.sec.gov/submissions/CIK{pad_cik(company['cik'])}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        if not recent:
            return []
        results = []
        accessions = recent.get("accessionNumber", [])
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        docs = recent.get("primaryDocument", [])
        descs = recent.get("primaryDocDescription", [])
        for i in range(len(accessions)):
            if dates[i] < date_from:
                continue
            accession_clean = accessions[i].replace("-", "")
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{company['cik']}/{accession_clean}/{docs[i]}"
            results.append({
                "id": f"cik-{accessions[i]}",
                "company": data.get("name", company["name"]),
                "form": forms[i],
                "filed": dates[i],
                "description": descs[i] if i < len(descs) else "",
                "url": doc_url,
                "source": "CIK",
                "accession": accessions[i],
                "cik": company["cik"],
            })
        return results
    except Exception as e:
        print(f"[ERROR] CIK fetch failed for {company['name']}: {e}")
        return []

# ── Name-based fetch ─────────────────────────────────────────────────────────
def fetch_by_name(company, date_from):
    url = "https://efts.sec.gov/LATEST/search-index"
    params = {"q": f'"{company["query"]}"', "dateRange": "custom", "startdt": date_from}
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        hits = resp.json().get("hits", {}).get("hits", [])
        results = []
        for h in hits:
            src = h.get("_source", {})
            results.append({
                "id": f"name-{h['_id']}",
                "company": src.get("entity_name", company["name"]),
                "form": src.get("form_type", "N/A"),
                "filed": src.get("file_date", ""),
                "description": src.get("file_description") or src.get("period_of_report", ""),
                "url": f"https://efts.sec.gov/LATEST/search-index?q=%22{requests.utils.quote(company['query'])}%22&dateRange=custom&startdt={src.get('file_date','')}",
                "source": "Name",
                "accession": h["_id"],
                "cik": None,
            })
        return results
    except Exception as e:
        print(f"[ERROR] Name fetch failed for {company['name']}: {e}")
        return []

# ── Deduplication ────────────────────────────────────────────────────────────
def deduplicate(filings):
    seen = {}
    for f in filings:
        key = f["accession"].replace("-", "")
        if key not in seen or f["source"] == "CIK":
            seen[key] = f
    return list(seen.values())

# ── Slack ────────────────────────────────────────────────────────────────────
def send_slack(filings):
    if not SLACK_WEBHOOK_URL:
        for f in filings:
            print(f"[NEW FILING] {f['company']} | {f['form']} | {f['filed']} | {f['source']}")
        return
    for f in filings:
        emoji = get_emoji(f["form"])
        text = (
            f"{emoji} *New SEC Filing Detected*\n"
            f">*Company:* {f['company']}\n"
            f">*Form:* `{f['form']}`"
            + (f" — _{f['description']}_" if f["description"] else "") + "\n"
            f">*Filed:* {f['filed']} · Found via: {f['source']}\n"
            f">*<{f['url']}|View Filing →>*"
        )
        try:
            r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
            r.raise_for_status()
            print(f"[SLACK] Sent: {f['company']} {f['form']} {f['filed']}")
        except Exception as e:
            print(f"[ERROR] Slack failed: {e}")
        time.sleep(0.5)

# ── Main Loop ─────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  🇧🇷 EDGAR Brazilian Companies Filing Monitor")
    print(f"  Watching {len(COMPANIES)} companies")
    print(f"  Poll interval: {POLL_INTERVAL_SECONDS}s")
    print("=" * 55)

    seen_ids = set()
    is_first_run = True

    while True:
        now = datetime.utcnow()
        date_from = (now - timedelta(days=DAYS_BACK_ON_START)).strftime("%Y-%m-%d")
        print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')} UTC] Polling EDGAR...")

        all_filings = []
        for company in COMPANIES:
            if company.get("cik"):
                all_filings.extend(fetch_by_cik(company, date_from))
            all_filings.extend(fetch_by_name(company, date_from))
            time.sleep(1)

        deduped = deduplicate(all_filings)

        new_filings = [f for f in deduped if f["id"] not in seen_ids]
        for f in deduped:
            seen_ids.add(f["id"])

        if is_first_run:
            print(f"[INFO] First run complete: {len(seen_ids)} filings loaded into memory. Alerting on NEW ones only from now.")
            is_first_run = False
        elif new_filings:
            print(f"[INFO] {len(new_filings)} new filing(s)! Sending Slack alerts...")
            send_slack(new_filings)
        else:
            print(f"[INFO] No new filings. ({len(seen_ids)} total tracked)")

        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
