#!/usr/bin/env python3
"""News briefing collector.

Fetches RSS feeds, Google Trends (AT/GB/US) and the weather for Wien,
Neuberg im Burgenland and Villach, and writes everything to data/briefing.json.
Runs on GitHub Actions (see .github/workflows/collect.yml).
The Claude routine then reads data/briefing.json, curates it and sends the e-mail.
"""
import html
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import feedparser
import requests

VIENNA = ZoneInfo("Europe/Vienna")
UA = "Mozilla/5.0 (compatible; NewsBriefingBot/1.0; +https://github.com/mmarlovics-ats/news-briefing)"
TIMEOUT = 25
OUT = Path(__file__).parent / "data" / "briefing.json"

# Window per section (hours back). Claude applies the exact period rules afterwards.
WINDOWS = {
    "austria": 40, "world": 40, "web": 40, "tech": 40,
    "music": 72, "art": 72, "gaming": 72, "pop": 72, "cycling": 96,
}
PER_SOURCE_CAP = 8
PER_SECTION_CAP = 70

# (source name, url)
FEEDS = {
    "austria": [
        ("ORF", "https://rss.orf.at/news.xml"),
        ("ORF Wien", "https://rss.orf.at/wien.xml"),
        ("Der Standard", "https://www.derstandard.at/rss/inland"),
        ("Der Standard", "https://www.derstandard.at/rss/wirtschaft"),
        ("Die Presse", "https://www.diepresse.com/rss"),
        ("Salzburger Nachrichten", "https://www.sn.at/xml/rss"),
        ("Kurier", "https://kurier.at/xml/rss"),
        ("profil", "https://www.profil.at/xml/rss"),
        ("APA-OTS Parlamentskorrespondenz", "https://www.ots.at/rss/index"),
        ("Falter", "https://www.falter.at/rss"),
        ("Wiener Zeitung", "https://www.wienerzeitung.at/rss"),
    ],
    "world": [
        ("The Guardian", "https://www.theguardian.com/world/europe-news/rss"),
        ("The Guardian", "https://www.theguardian.com/world/rss"),
        ("The New York Times", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
        ("The New York Times", "https://rss.nytimes.com/services/xml/rss/nyt/Europe.xml"),
        ("BBC News", "https://feeds.bbci.co.uk/news/world/rss.xml"),
        ("The Economist", "https://www.economist.com/latest/rss.xml"),
        ("The Economist", "https://www.economist.com/international/rss.xml"),
        ("Politico Europe", "https://www.politico.eu/feed/"),
        ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
        ("Le Monde", "https://www.lemonde.fr/en/rss/une.xml"),
        ("DW", "https://rss.dw.com/rdf/rss-en-all"),
        ("NZZ", "https://www.nzz.ch/international.rss"),
        ("Der Spiegel", "https://www.spiegel.de/ausland/index.rss"),
        ("Die Zeit", "https://newsfeed.zeit.de/politik/index"),
        ("Süddeutsche Zeitung", "https://rss.sueddeutsche.de/rss/Politik"),
        ("FAZ", "https://www.faz.net/rss/aktuell/politik/ausland/"),
        ("Tagesschau", "https://www.tagesschau.de/index~rss2.xml"),
        ("Der Standard", "https://www.derstandard.at/rss/international"),
        ("Euronews", "https://www.euronews.com/rss?format=mrss"),
        ("Sky News", "https://feeds.skynews.com/feeds/rss/world.xml"),
        ("France 24", "https://www.france24.com/en/rss"),
        ("NPR", "https://feeds.npr.org/1004/rss.xml"),
        ("ABC Australia", "https://www.abc.net.au/news/feed/2942460/rss.xml"),
        ("Channel NewsAsia", "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6511"),
        ("The Independent", "https://www.independent.co.uk/news/world/rss"),
        ("Irish Times", "https://www.irishtimes.com/arc/outboundfeeds/feed/?outputType=xml"),
        ("CBC", "https://www.cbc.ca/webfeed/rss/rss-world"),
        ("The Guardian (Reuters-Themen)", "https://www.theguardian.com/business/rss"),
    ],
    "web": [
        ("heise", "https://www.heise.de/rss/heise-top-atom.xml"),
        ("netzpolitik.org", "https://netzpolitik.org/feed/"),
        ("Golem", "https://rss.golem.de/rss.php?feed=RSS2.0"),
        ("t3n", "https://t3n.de/rss.xml"),
        ("ComputerBase", "https://www.computerbase.de/rss/news.xml"),
        ("Der Standard", "https://www.derstandard.at/rss/web"),
        ("The Verge", "https://www.theverge.com/rss/index.xml"),
        ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/index"),
        ("Wired", "https://www.wired.com/feed/rss"),
        ("404 Media", "https://www.404media.co/rss/"),
        ("TechCrunch", "https://techcrunch.com/feed/"),
        ("Engadget", "https://www.engadget.com/rss.xml"),
        ("The Guardian", "https://www.theguardian.com/uk/technology/rss"),
        ("The Register", "https://www.theregister.com/headlines.atom"),
    ],
    "tech": [
        ("heise", "https://www.heise.de/rss/heise-top-atom.xml"),
        ("Golem", "https://rss.golem.de/rss.php?feed=RSS2.0"),
        ("t3n", "https://t3n.de/rss.xml"),
        ("ComputerBase", "https://www.computerbase.de/rss/news.xml"),
        ("The Verge", "https://www.theverge.com/rss/index.xml"),
        ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/index"),
        ("Wired", "https://www.wired.com/feed/rss"),
        ("MIT Technology Review", "https://www.technologyreview.com/feed/"),
        ("The New York Times", "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml"),
        ("Rest of World", "https://restofworld.org/feed/latest"),
        ("TechCrunch", "https://techcrunch.com/feed/"),
        ("Engadget", "https://www.engadget.com/rss.xml"),
    ],
    "music": [
        ("Pitchfork", "https://pitchfork.com/rss/news/"),
        ("The Quietus", "https://thequietus.com/feed/"),
        ("The Guardian", "https://www.theguardian.com/music/rss"),
        ("Music Business Worldwide", "https://www.musicbusinessworldwide.com/feed/"),
        ("NME", "https://www.nme.com/news/music/feed"),
        ("Stereogum", "https://www.stereogum.com/feed/"),
        ("Resident Advisor", "https://ra.co/xml/news.xml"),
        ("Billboard", "https://www.billboard.com/feed/"),
        ("Rolling Stone", "https://www.rollingstone.com/music/music-news/feed/"),
        ("Consequence", "https://consequence.net/feed/"),
        ("DIY", "https://diymag.com/feed"),
        ("Mixmag", "https://mixmag.net/rss.xml"),
        ("Der Standard", "https://www.derstandard.at/rss/kultur"),
    ],
    "art": [
        ("The Art Newspaper", "https://www.theartnewspaper.com/rss.xml"),
        ("Artnet", "https://news.artnet.com/feed"),
        ("ARTnews", "https://www.artnews.com/feed/"),
        ("Hyperallergic", "https://hyperallergic.com/feed/"),
        ("Monopol", "https://www.monopol-magazin.de/rss.xml"),
        ("The Guardian", "https://www.theguardian.com/artanddesign/rss"),
        ("The Guardian", "https://www.theguardian.com/books/rss"),
        ("The New York Times", "https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml"),
        ("Der Standard", "https://www.derstandard.at/rss/kultur"),
        ("Dezeen", "https://www.dezeen.com/feed/"),
    ],
    "gaming": [
        ("Eurogamer", "https://www.eurogamer.net/feed"),
        ("GamesIndustry.biz", "https://www.gamesindustry.biz/feed"),
        ("Rock Paper Shotgun", "https://www.rockpapershotgun.com/feed"),
        ("The Verge", "https://www.theverge.com/rss/games/index.xml"),
        ("Polygon", "https://www.polygon.com/rss/index.xml"),
        ("PC Gamer", "https://www.pcgamer.com/rss/"),
        ("Game Developer", "https://www.gamedeveloper.com/rss.xml"),
        ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/gaming"),
        ("Video Games Chronicle", "https://www.videogameschronicle.com/feed/"),
        ("Nintendo Life", "https://www.nintendolife.com/feeds/latest"),
        ("IGN", "https://www.ign.com/rss/v2/articles/feed"),
    ],
    "pop": [
        ("Variety", "https://variety.com/feed/"),
        ("The Hollywood Reporter", "https://www.hollywoodreporter.com/feed/"),
        ("Deadline", "https://deadline.com/feed/"),
        ("IndieWire", "https://www.indiewire.com/feed/"),
        ("Vulture", "https://www.vulture.com/rss/index.xml"),
        ("The Guardian", "https://www.theguardian.com/film/rss"),
        ("The Guardian", "https://www.theguardian.com/tv-and-radio/rss"),
        ("The Guardian", "https://www.theguardian.com/culture/rss"),
        ("Collider", "https://collider.com/feed/"),
        ("SlashFilm", "https://www.slashfilm.com/feed/"),
        ("The A.V. Club", "https://www.avclub.com/rss"),
        ("Der Standard", "https://www.derstandard.at/rss/kultur"),
    ],
    "cycling": [
        ("Escape Collective", "https://escapecollective.com/feed/"),
        ("The Radavist", "https://theradavist.com/feed/"),
        ("Bikepacking.com", "https://bikepacking.com/feed/"),
        ("Cyclingnews", "https://www.cyclingnews.com/feeds.xml"),
        ("VeloNews", "https://velo.outsideonline.com/feed/"),
        ("road.cc", "https://road.cc/rss"),
        ("BikeRadar", "https://www.bikeradar.com/feed"),
        ("Cycling Weekly", "https://www.cyclingweekly.com/feeds/all"),
    ],
}

PLACES = {
    "Wien": (48.21, 16.37),
    "Neuberg im Burgenland": (47.167, 16.267),
    "Villach": (46.61, 13.85),
}

# Titles/links that are never news (ads, deals, listicles)
JUNK = re.compile(
    r"^(anzeige|sponsored)\b|black friday|deal[s]?\b|angebot|gutschein|coupon|"
    r"\bbest .* (of|to buy)\b|\bwin!|giveaway|horoskop",
    re.I,
)


def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/rss+xml, application/xml, text/xml, */*"})
    return s


def clean(text, limit=300):
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def entry_time(e):
    for key in ("published_parsed", "updated_parsed"):
        t = e.get(key)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc)
    return None


def norm_title(t):
    return re.sub(r"[^a-z0-9äöüß]+", "", t.lower())[:80]


def fetch_feed(args):
    section, name, url = args
    status = {"section": section, "source": name, "url": url, "ok": False, "items": 0, "error": ""}
    try:
        r = session().get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            status["error"] = f"HTTP {r.status_code}"
            return status, []
        d = feedparser.parse(r.content)
        items = []
        for e in d.entries:
            link = (e.get("link") or "").strip()
            title = clean(e.get("title"), 300)
            if not link or not title:
                continue
            summary = clean(e.get("summary") or e.get("description") or "", 300)
            t = entry_time(e)
            items.append({
                "title": title,
                "link": link,
                "source": name,
                "published": t.isoformat() if t else None,
                "summary": summary,
            })
        status["ok"] = True
        status["items"] = len(items)
        return status, items
    except Exception as ex:  # noqa: BLE001
        status["error"] = str(ex)[:120]
        return status, []


def collect_news(now):
    jobs = [(sec, n, u) for sec, feeds in FEEDS.items() for n, u in feeds]
    with ThreadPoolExecutor(max_workers=12) as ex:
        results = list(ex.map(fetch_feed, jobs))
    statuses, per_section = [], {s: [] for s in FEEDS}
    for (sec, _n, _u), (status, items) in zip(jobs, results):
        statuses.append(status)
        per_section[sec].extend(items)

    out = {}
    for sec, items in per_section.items():
        cutoff = now - timedelta(hours=WINDOWS[sec])
        seen_t, seen_l, kept, per_src = set(), set(), [], {}
        items.sort(key=lambda i: i["published"] or "", reverse=True)
        for i in items:
            if i["published"] is None or datetime.fromisoformat(i["published"]) < cutoff:
                continue
            if JUNK.search(i["title"]):
                continue
            nt = norm_title(i["title"])
            if nt in seen_t or i["link"] in seen_l:
                continue
            if per_src.get(i["source"], 0) >= PER_SOURCE_CAP:
                continue
            seen_t.add(nt)
            seen_l.add(i["link"])
            per_src[i["source"]] = per_src.get(i["source"], 0) + 1
            kept.append(i)
            if len(kept) >= PER_SECTION_CAP:
                break
        out[sec] = kept
    return out, statuses


def parse_trends(content):
    """Parse the Google Trends RSS with ElementTree (feedparser keeps only one news item)."""
    from email.utils import parsedate_to_datetime
    import xml.etree.ElementTree as ET

    def local(tag):
        return tag.rsplit("}", 1)[-1]

    root = ET.fromstring(content)
    items = []
    for item in root.iter("item"):
        row = {"term": "", "traffic": "", "published": None, "news": []}
        for child in item:
            name = local(child.tag)
            if name == "title":
                row["term"] = (child.text or "").strip()
            elif name == "approx_traffic":
                row["traffic"] = (child.text or "").strip()
            elif name == "pubDate" and child.text:
                try:
                    row["published"] = parsedate_to_datetime(child.text).astimezone(timezone.utc).isoformat()
                except Exception:  # noqa: BLE001
                    pass
            elif name == "news_item":
                n = {local(c.tag): (c.text or "").strip() for c in child}
                if n.get("news_item_url"):
                    row["news"].append({
                        "title": clean(n.get("news_item_title"), 200),
                        "source": clean(n.get("news_item_source"), 80),
                        "url": n["news_item_url"],
                    })
        if row["term"]:
            items.append(row)
        if len(items) >= 10:
            break
    return items


def fetch_trends(geo, now):
    """Google Trends RSS. Plain URL is often a stale cache -> cache-buster + freshness check."""
    stamp = now.astimezone(VIENNA).strftime("%Y%m%d%H%M")
    for attempt, suffix in enumerate(["", "a", "b", "c"]):
        url = f"https://trends.google.com/trending/rss?geo={geo}&hours=24&cb={stamp}{suffix}"
        try:
            r = session().get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                continue
            items = parse_trends(r.content)
            if not items:
                continue
            newest = max((datetime.fromisoformat(i["published"]) for i in items if i["published"]), default=None)
            if newest and now - newest < timedelta(hours=36):
                return {"fresh": True, "attempt": attempt + 1, "items": items}
        except Exception:  # noqa: BLE001
            continue
    return {"fresh": False, "attempt": 4, "items": []}


def fetch_weather(target_date):
    out = {}
    for place, (lat, lon) in PLACES.items():
        url = f"https://api.brightsky.dev/weather?lat={lat}&lon={lon}&date={target_date}&tz=Europe/Vienna"
        data = None
        for _ in range(2):
            try:
                r = session().get(url, timeout=TIMEOUT)
                if r.status_code == 200:
                    data = r.json()
                    break
            except Exception:  # noqa: BLE001
                continue
        if not data:
            out[place] = None
            continue
        rows = data.get("weather", [])
        day = [w for w in rows if w["timestamp"][:10] == target_date]

        def at(h):
            for w in day:
                if w["timestamp"][11:13] == f"{h:02d}":
                    return {
                        "temp_c": w.get("temperature"),
                        "precip_mm": w.get("precipitation"),
                        "precip_prob_pct": w.get("precipitation_probability"),
                        "wind_kmh": w.get("wind_speed"),
                        "condition": w.get("condition"),
                        "icon": w.get("icon"),
                    }
            return None

        temps = [w["temperature"] for w in day if w.get("temperature") is not None]
        out[place] = {
            "07:00": at(7), "12:00": at(12), "18:00": at(18),
            "min_c": min(temps) if temps else None,
            "max_c": max(temps) if temps else None,
            "source": "Bright Sky (DWD)",
        }
    return out


def main():
    now = datetime.now(timezone.utc)
    vienna = now.astimezone(VIENNA)
    # The workflow fires at two UTC hours per run to cover summer/winter time.
    # Only the run that lands on the intended Vienna hour proceeds (unless forced).
    if not os.environ.get("FORCE") and vienna.hour not in (4, 17):
        print(f"Vienna time {vienna:%H:%M} - not a collection hour, exiting.")
        return 0

    run = "morning" if vienna.hour < 12 else "evening"
    target = vienna.date() if run == "morning" else (vienna + timedelta(days=1)).date()

    news, statuses = collect_news(now)
    trends = {geo: fetch_trends(geo, now) for geo in ("AT", "GB", "US")}
    weather = fetch_weather(target.isoformat())

    ok = sum(1 for s in statuses if s["ok"])
    payload = {
        "generated_at": now.isoformat(),
        "generated_vienna": vienna.strftime("%Y-%m-%d %H:%M"),
        "run": run,
        "weather_date": target.isoformat(),
        "weather": weather,
        "trends": trends,
        "news": news,
        "feed_status": {"ok": ok, "total": len(statuses), "failed": [s for s in statuses if not s["ok"]]},
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    counts = {k: len(v) for k, v in news.items()}
    print(f"feeds ok {ok}/{len(statuses)}; items per section {counts}")
    print("trends fresh:", {g: t["fresh"] for g, t in trends.items()})
    print("weather ok:", {p: bool(w) for p, w in weather.items()})
    return 0


if __name__ == "__main__":
    sys.exit(main())
