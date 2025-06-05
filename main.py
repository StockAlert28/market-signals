#!/usr/bin/env python3
# ---------- market-signals v2  ---------------------------------------
# Sends six different early-warning feeds to your downstream pipeline
# (database → CSV → Telegram bot handled elsewhere).
# --------------------------------------------------------------------

import os, datetime, sqlite3, csv, json, time, requests, feedparser
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import praw

# ========== 0.  MEMORY: keep a ".last_seen.json" anti-spam ledger ====
MEMFILE = ".last_seen.json"

def _load_seen():
    try:
        with open(MEMFILE, "r") as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()

def _save_seen(seen_set):
    with open(MEMFILE, "w") as f:
        json.dump(sorted(list(seen_set)), f)

seen = _load_seen()

def is_new(uid: str) -> bool:
    "Return True the first time we see uid, False afterwards."
    if uid in seen:
        return False
    seen.add(uid)
    return True

# ========== 1.  BASIC SETUP ==========================================
FINN   = os.getenv("FINNHUB_KEY")
TW_BEAR= os.getenv("TWITTER_BEARER")       # for Twitter recent-search
now    = datetime.datetime.utcnow().date()

# DB / CSV
db  = sqlite3.connect("signals.db")
cur = db.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS signals(
               ts TEXT, source TEXT, ticker TEXT,
               headline TEXT, extra TEXT)""")

def push(row, uid):
    "Insert row into DB & CSV if it's new."
    if not is_new(uid):
        return
    cur.execute("INSERT INTO signals VALUES (?,?,?,?,?)", row)
    with open("signals.csv", "a", newline="") as f:
        csv.writer(f).writerow(row)

# Sentiment
analyzer = SentimentIntensityAnalyzer()

# Reddit
reddit = praw.Reddit(client_id=os.getenv("REDDIT_ID"),
                     client_secret=os.getenv("REDDIT_SECRET"),
                     user_agent=os.getenv("REDDIT_USERAGENT"))

# ========== 2.  SEC FILINGS  (Form 4 & 8-K) ==========================
print("• SEC feeds…")
for form in ("4", "8-K"):
    rss = ( "https://www.sec.gov/cgi-bin/browse-edgar"
            f"?action=getcurrent&type={form}&count=100&output=atom&owner=include" )
    for entry in feedparser.parse(rss).entries:
        uid = entry.id                                   # Permanent Atom GUID
        ticker = entry.title.split()[0]
        push([entry.updated, f"SEC{form}", ticker, entry.title, entry.link],
             uid)

# ========== 3.  EARNINGS IN THE NEXT 72 H ========== ================
print("• Earnings calendar…")
r = requests.get("https://finnhub.io/api/v1/calendar/earnings",
                 params={"from": now, "to": now + datetime.timedelta(days=2),
                         "token": FINN}, timeout=10)
for row in r.json().get("earningsCalendar", []):
    uid = f"earn-{row['symbol']}-{row['date']}"
    push([row["date"], "EARN", row["symbol"],
          f"Earnings {row['date']} (est EPS {row.get('epsEstimate')})", ""],
         uid)

# ========== 4.  BUSINESS WIRE PRESS RELEASES (keyword filter) =======
print("• Business Wire PR…")
BW_RSS = ("https://services.businesswire.com/rss/home/?"
          "rssQuery=merger%20OR%20guidance%20OR%20contract%20award")
for e in feedparser.parse(BW_RSS).entries:
    uid = e.id
    push([e.published, "PR", "", e.title, e.link], uid)

# ========== 5.  UNUSUAL OPTIONS  (top rows scraped from Barchart) ====
print("• Unusual options…")
try:
    soup = BeautifulSoup(requests.get(
        "https://www.barchart.com/options/unusual-activity", timeout=10).text,
        "html.parser")
    for row in soup.select("table tbody tr")[:15]:           # first 15 rows
        cols = [c.get_text(strip=True) for c in row.select("td")]
        if len(cols) < 4: continue
        sym, vol = cols[0], cols[3]
        uid = f"opt-{sym}-{vol}"
        push([str(datetime.datetime.utcnow()), "OPT", sym,
              f"Unusual option volume {vol}", ""], uid)
except Exception as e:
    print("   (options scrape failed)", e)

# ========== 6.  SOCIAL BUZZ  ========================================
# 6a) Twitter recent-search (free 10-query / 15 min on the new Basic plan)
print("• Twitter search…")
if TW_BEAR:
    TW_QUERY  = "fda approval OR executive order OR $SPY OR tariff"
    TW_URL    = "https://api.twitter.com/2/tweets/search/recent"
    headers   = {"Authorization": f"Bearer {TW_BEAR}"}
    params    = {"query": TW_QUERY, "max_results": 20,
                 "tweet.fields": "created_at"}
    try:
        tw = requests.get(TW_URL, headers=headers, params=params,
                          timeout=10).json().get("data", [])
        for t in tw:
            uid = f"tw-{t['id']}"
            ts  = t["created_at"]
            text= t["text"][:150].replace("\n", " ")
            push([ts, "TWIT", "", text,
                  f"https://twitter.com/i/web/status/{t['id']}"], uid)
    except Exception as e:
        print("   (Twitter fetch failed)", e)

# 6b) Reddit new posts (WSB + Stocks)
print("• Reddit stream…")
for post in reddit.subreddit("wallstreetbets+stocks").new(limit=25):
    uid = f"rd-{post.id}"
    if not is_new(uid):
        continue
    score = analyzer.polarity_scores(post.title)["compound"]
    push([datetime.datetime.utcfromtimestamp(post.created_utc),
          "REDDIT", "", post.title,
          f"sent={score:.2f}|url={post.shortlink}"], uid)

# ========== 7.  POLICY-BOMB HEADLINES (Google News keywords) ========
print("• Policy bomb headlines…")
GN_RSS = ("https://news.google.com/rss/search?"
          "q=tariff+OR+antitrust+investigation+OR+rate+hike+site:reuters.com")
for e in feedparser.parse(GN_RSS).entries:
    uid = e.id
    score = analyzer.polarity_scores(e.title)["compound"]
    push([e.published, "NEWS", "", e.title, f"sent={score:.2f}"], uid)

# ========== 8.  CLOSE ===============================================
db.commit()
db.close()
_save_seen(seen)
print("✅ All six feeds processed without spam duplicates")
