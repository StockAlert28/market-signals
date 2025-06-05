#!/usr/bin/env python3
# market-signals – one-stop data pull for Telegram alerts
# -------------------------------------------------------
import os, datetime, sqlite3, csv, json, time, random, requests, feedparser
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import praw

# ---------- 0.  GLOBALS --------------------------------------------
HTTP_TIMEOUT = 8
UA_HEADER    = {"User-Agent": "Mozilla/5.0 (GitHub Actions bot)"}
MEMFILE      = ".last_seen.json"

# ---------- 1.  MEMORY (anti-spam) ---------------------------------
def load_seen():
    try:
        return set(json.load(open(MEMFILE)))
    except FileNotFoundError:
        return set()

def save_seen(s):
    json.dump(sorted(s), open(MEMFILE, "w"))

seen = load_seen()

def is_new(uid: str) -> bool:
    if uid in seen:
        return False
    seen.add(uid)
    return True

# ---------- 2.  HELPERS --------------------------------------------
def get_feed(url: str, tries: int = 3):
    """Download RSS/Atom with timeout + polite UA; returns feedparser object."""
    for n in range(tries):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT, headers=UA_HEADER)
            r.raise_for_status()
            return feedparser.parse(r.content)
        except Exception as e:
            if n == tries - 1:
                print("   (feed failed)", url[:60], e)
                return feedparser.parse(b"")
            time.sleep(3 * (2 ** n) + random.random())

def push(row, uid):
    """Insert into DB & CSV only if uid not seen before."""
    if not is_new(uid):
        return
    cur.execute("INSERT INTO signals VALUES (?,?,?,?,?)", row)
    csv.writer(csvfile).writerow(row)

# ---------- 3.  SETUP ----------------------------------------------
FINN_KEY = os.getenv("FINNHUB_KEY")
TW_BEAR  = os.getenv("TWITTER_BEARER")
now      = datetime.datetime.utcnow().date()

db  = sqlite3.connect("signals.db")
cur = db.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS signals(
               ts TEXT, source TEXT, ticker TEXT, headline TEXT, extra TEXT)""")
csvfile = open("signals.csv", "a", newline="")

analyzer = SentimentIntensityAnalyzer()

# Reddit setup (skip if creds absent)
reddit = None
if os.getenv("REDDIT_ID") and os.getenv("REDDIT_SECRET"):
    reddit = praw.Reddit(client_id=os.getenv("REDDIT_ID"),
                         client_secret=os.getenv("REDDIT_SECRET"),
                         user_agent=os.getenv("REDDIT_USERAGENT", "reddit-watcher"))

# ---------- 4.  FEEDS ------------------------------------------------
print("• SEC filings")
for form in ("4", "8-K"):
    url = ("https://www.sec.gov/cgi-bin/browse-edgar?"
           f"action=getcurrent&type={form}&owner=include&count=100&output=atom")
    for e in get_feed(url).entries:
        uid = e.id
        ticker = e.title.split()[0]
        push([e.updated, f"SEC{form}", ticker, e.title, e.link], uid)

print("• Earnings calendar")
if FINN_KEY:
    resp = requests.get("https://finnhub.io/api/v1/calendar/earnings",
                        params={"from": now, "to": now + datetime.timedelta(days=2),
                                "token": FINN_KEY},
                        timeout=HTTP_TIMEOUT, headers=UA_HEADER)
    for row in resp.json().get("earningsCalendar", []):
        uid = f"earn-{row['symbol']}-{row['date']}"
        push([row["date"], "EARN", row["symbol"],
              f"Earnings {row['date']} (est EPS {row.get('epsEstimate')})", ""], uid)

print("• Business Wire PR")
BW_RSS = ("https://services.businesswire.com/rss/home/?"
          "rssQuery=merger%20OR%20guidance%20OR%20contract%20award")
for e in get_feed(BW_RSS).entries:
    uid = e.id
    push([e.published, "PR", "", e.title, e.link], uid)

print("• Unusual options (Barchart)")
try:
    soup = BeautifulSoup(requests.get(
        "https://www.barchart.com/options/unusual-activity",
        timeout=HTTP_TIMEOUT, headers=UA_HEADER).text, "html.parser")
    for row in soup.select("table tbody tr")[:15]:
        cols = [c.get_text(strip=True) for c in row.select("td")]
        if len(cols) < 4:
            continue
        sym, vol = cols[0], cols[3]
        uid = f"opt-{sym}-{vol}"
        push([str(datetime.datetime.utcnow()), "OPT", sym,
              f"Unusual option volume {vol}", ""], uid)
except Exception as e:
    print("   (options scrape failed)", e)

print("• Twitter recent search")
if TW_BEAR:
    try:
        tw = requests.get("https://api.twitter.com/2/tweets/search/recent",
                          headers={"Authorization": f"Bearer {TW_BEAR}"},
                          params={"query": "fda approval OR executive order OR tariff",
                                  "max_results": 25, "tweet.fields": "created_at"},
                          timeout=HTTP_TIMEOUT).json().get("data", [])
        for t in tw:
            uid = f"tw-{t['id']}"
            push([t["created_at"], "TWIT", "",
                  t["text"].replace("\n", " ")[:150],
                  f"https://twitter.com/i/web/status/{t['id']}"], uid)
    except Exception as e:
        print("   (Twitter fetch failed)", e)

print("• Reddit stream")
if reddit:
    try:
        for post in reddit.subreddit("wallstreetbets+stocks").new(limit=25):
            uid = f"rd-{post.id}"
            if not is_new(uid):
                continue
            s = analyzer.polarity_scores(post.title)["compound"]
            push([datetime.datetime.utcfromtimestamp(post.created_utc),
                  "REDDIT", "", post.title,
                  f"sent={s:.2f}|url={post.shortlink}"], uid)
    except Exception as e:
        print("   (Reddit fetch failed)", e)

print("• Policy headlines")
GN_RSS = ("https://news.google.com/rss/search?"
          "q=tariff+OR+antitrust+investigation+OR+rate+hike+site:reuters.com")
for e in get_feed(GN_RSS).entries:
    uid = e.id
    score = analyzer.polarity_scores(e.title)["compound"]
    push([e.published, "NEWS", "", e.title, f"sent={score:.2f}"], uid)

# ---------- 5.  CLOSE ----------------------------------------------
db.commit()
db.close()
csvfile.close()
save_seen(seen)
print("✅ Feeds processed, duplicates suppressed")
