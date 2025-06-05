import os, datetime, sqlite3, csv, requests, feedparser, praw, pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1) --- SETUP -------------------------------------------------
FINN = os.getenv("FINNHUB_KEY")
now  = datetime.datetime.utcnow().date()
db   = sqlite3.connect("signals.db")
cur  = db.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS signals(
   ts TEXT, source TEXT, ticker TEXT, headline TEXT, extra TEXT)""")

analyzer = SentimentIntensityAnalyzer()

# Reddit
reddit = praw.Reddit(client_id=os.getenv("REDDIT_ID"),
                     client_secret=os.getenv("REDDIT_SECRET"),
                     user_agent=os.getenv("REDDIT_USERAGENT"))

def save(row):
    cur.execute("INSERT INTO signals VALUES (?,?,?,?,?)", row)
    with open("signals.csv","a", newline="") as f:
        csv.writer(f).writerow(row)

# 2) --- SEC insider buys (Form 4) ------------------------------
feed = feedparser.parse("https://www.sec.gov/cgi-bin/browse-edgar?"
                        "action=getcurrent&type=4&owner=include&count=100&output=atom")
for entry in feed.entries:
    ticker = entry.title.split(" ")[0]
    save([entry.updated, "SEC4", ticker, entry.title, entry.link])

# 3) --- SEC surprise 8-K --------------------------------------
feed = feedparser.parse("https://www.sec.gov/cgi-bin/browse-edgar?"
                        f"action=getcurrent&type=8-K&count=100&output=atom")
for e in feed.entries:
    save([e.updated, "SEC8K", e.title.split(' ')[0], e.title, e.link])

# 4) --- Earnings calendar -------------------------------------
earn = requests.get("https://finnhub.io/api/v1/calendar/earnings",
                    params={"from": now, "token": FINN}).json().get("earningsCalendar",[])
for row in earn:
    save([row["date"], "EARN", row["symbol"],
          f"Earnings in {row['date']} (est EPS {row['epsEstimate']})", ""])

# 5) --- Tariff headlines --------------------------------------
feed = feedparser.parse("https://news.google.com/rss/search?"
                        "q=Trump+tariff+steel+aluminum")
for e in feed.entries:
    score = analyzer.polarity_scores(e.title)["compound"]
    save([e.published, "NEWS", "", e.title, f"sent={score:.2f}"])

# 6) --- Reddit hype stream (just latest 20 posts) -------------
for post in reddit.subreddit("wallstreetbets+stocks").new(limit=20):
    s = analyzer.polarity_scores(post.title)["compound"]
    save([datetime.datetime.utcfromtimestamp(post.created_utc),
          "REDDIT", "", post.title, f"sent={s:.2f}|url={post.shortlink}"])

db.commit()
db.close()
print("✅ done")
