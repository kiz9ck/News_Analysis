import functions_framework
import feedparser
import hashlib
import json
import logging
import os
import asyncio
from datetime import datetime, timezone, timedelta

import httpx
from bs4 import BeautifulSoup
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLE_ID = os.environ["BQ_TABLE_ID"]
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "1"))

MIN_SUMMARY_LEN = 150

REAL_ASSETS = ["BTC", "ETH"]

bq_client = bigquery.Client()

RSS_FEEDS = [
    "https://thedefiant.io/feed/",
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://cointelegraph.com/rss",
    "https://cryptopotato.com/feed/",
    "https://cryptoslate.com/feed/",
    "https://cryptonews.com/news/feed/",
    "https://finance.yahoo.com/news/rssindex",
    "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "https://benjaminion.xyz/newineth2/rss_feed.xml",
    "https://decrypt.co/feed",
    "https://www.theblock.co/rss.xml",
    "https://beincrypto.com/feed/",
    "https://dailyhodl.com/feed/",
    "https://bitcoinmagazine.com/.rss/full/",
    "https://www.newsbtc.com/feed/",
    "https://ambcrypto.com/feed/",
    "https://u.today/rss",
    "https://www.investing.com/rss/news_301.rss",
    "https://watcher.guru/news/feed",
    "https://blockworks.co/feed",
    "https://protos.com/feed/",
    "https://www.dlnews.com/arc/outboundfeeds/rss/",
    "https://cryptobriefing.com/feed/",
    "https://coingape.com/feed/",
    "https://cryptodaily.co.uk/feed",
    "https://zycrypto.com/feed/",
    "https://forkast.news/feed/",
    "https://cryptonewsz.com/feed/",
    "https://blog.chain.link/rss/",
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://news.google.com/rss/search?q=SEC+crypto+regulation&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=CFTC+crypto&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=bitcoin+ETF+regulation&hl=en-US&gl=US&ceid=US:en",
    "https://99bitcoins.com/feed/",
    "https://coinjournal.net/feed/",
    "https://crypto.news/feed/",
    "https://bitcoinist.com/feed/",
    "https://www.axios.com/feeds/feed.rss",
]

ASSET_KEYWORDS: dict[str, list[str]] = {
    "ETH": [
        "eth",
        "ethereum",
        "ether",
        "vitalik",
        "buterin",
        "erc-20",
        "erc20",
        "erc-721",
        "erc721",
        "dencun",
        "shanghai",
        "evm",
        "eip",
        "gwei",
        "gas fee",
        "layer 2",
        "l2",
        "rollup",
        "zk-rollup",
        "optimism",
        "arbitrum",
        "base network",
        "polygon",
        "defi",
        "staking",
        "lido",
        "uniswap",
        "eth etf",
        "ethereum etf",
        "spot eth",
    ],
    "BTC": [
        "btc",
        "bitcoin",
        "satoshi",
        "nakamoto",
        "lightning network",
        "halving",
        "bitcoin etf",
        "spot btc",
        "bitcoin miner",
        "bitcoin mining",
        "sats",
        "taproot",
        "ordinals",
        "runes",
        "microstrategy",
        "blackrock bitcoin",
    ],
    "SOL": [
        "sol",
        "solana",
        "solana network",
        "sealevel",
        "proof of history",
        "solana etf",
        "jupiter",
        "raydium",
        "phantom wallet",
        "solana nft",
        "solana meme",
        "bonk",
        "solana validator",
        "solana congestion",
    ],
    "BNB": [
        "bnb",
        "binance coin",
        "binance smart chain",
        "bsc",
        "bnb chain",
        "pancakeswap",
        "binance exchange",
        "cz binance",
        "changpeng zhao",
        "bnb burn",
        "binance launchpad",
    ],
    "MACRO": [
        "federal reserve",
        "fed rate",
        "interest rate",
        "cpi",
        "inflation",
        "recession",
        "sec crypto",
        "crypto regulation",
        "crypto ban",
        "etf approval",
        "spot etf",
        "tether",
        "usdt",
        "usdc",
        "stablecoin",
        "crypto hack",
        "exploit",
        "rug pull",
        "institutional",
        "blackrock",
        "fidelity",
        "vanguard",
    ],
}

_ALL_KEYWORDS: set[str] = {kw for kws in ASSET_KEYWORDS.values() for kw in kws}


def match_assets(text: str) -> list[str]:
    if not text:
        return []
    text_lower = text.lower()
    if not any(kw in text_lower for kw in _ALL_KEYWORDS):
        return []

    matched = [
        ticker
        for ticker, keywords in ASSET_KEYWORDS.items()
        if any(kw in text_lower for kw in keywords)
    ]

    if "MACRO" in matched:
        matched.remove("MACRO")
        for asset in REAL_ASSETS:
            if asset not in matched:
                matched.append(asset)

    return matched


def make_news_id(link: str, title: str) -> str:
    return hashlib.sha256(f"{link}|{title}".encode()).hexdigest()[:16]


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    return BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)


def parse_date(entry) -> datetime:
    for attr in ("published_parsed", "updated_parsed"):
        val = entry.get(attr)
        if val:
            try:
                return datetime(*val[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def extract_source(feed_url: str) -> str:
    try:
        host = feed_url.split("://")[1].split("/")[0]
        parts = host.replace("www.", "").split(".")
        return ".".join(parts[-2:])
    except Exception:
        return feed_url


async def fetch_feed_async(
    client: httpx.AsyncClient, url: str, timeout: int = 15
) -> tuple[str, feedparser.FeedParserDict | None]:
    try:
        resp = await client.get(
            url,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "InvestTracker/2.0"},
        )
        resp.raise_for_status()
        feed = feedparser.parse(resp.text)
        if feed.bozo and not feed.entries:
            logger.warning("Malformed feed: %s", url)
            return url, None
        return url, feed
    except Exception as e:
        logger.warning("Error fetching %s: %s", url, e)
        return url, None


async def gather_all_feeds(
    client: httpx.AsyncClient, urls: list[str]
) -> list[tuple[str, feedparser.FeedParserDict | None]]:
    tasks = [fetch_feed_async(client, url) for url in urls]
    return await asyncio.gather(*tasks)


async def fetch_article_text(
    client: httpx.AsyncClient, url: str, timeout: int = 15
) -> str:
    try:
        resp = await client.get(
            url,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "InvestTracker/2.0"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup(
            [
                "script",
                "style",
                "nav",
                "header",
                "footer",
                "aside",
                "form",
                "noscript",
                "iframe",
            ]
        ):
            tag.decompose()

        for selector in (
            "article",
            '[class*="article-body"]',
            '[class*="article-content"]',
            '[class*="post-content"]',
            '[class*="entry-content"]',
            '[class*="story-body"]',
            '[class*="content-body"]',
            "main",
        ):
            container = soup.select_one(selector)
            if container:
                text = container.get_text(separator=" ", strip=True)
                if len(text) > MIN_SUMMARY_LEN:
                    return text[:5000]

        body = soup.find("body")
        if body:
            return body.get_text(separator=" ", strip=True)[:5000]

    except Exception as e:
        logger.warning("fetch_article_text failed [%s]: %s", url, e)

    return ""


async def enrich_entries_with_full_text(
    client: httpx.AsyncClient, entries: list[dict]
) -> None:
    targets = [e for e in entries if len(e["content"]) < MIN_SUMMARY_LEN]
    if not targets:
        return

    logger.info("Enriching %d entries with full article text...", len(targets))

    tasks = [fetch_article_text(client, e["url"]) for e in targets]
    results = await asyncio.gather(*tasks)

    enriched = 0
    for entry, full_text in zip(targets, results):
        if full_text and len(full_text) > len(entry["content"]):
            entry["content"] = full_text
            new_assets = match_assets(entry["title"] + " " + full_text)
            if new_assets:
                entry["assets"] = new_assets
            enriched += 1

    logger.info("Enriched %d / %d entries", enriched, len(targets))


def parse_feed_entries(
    feed: feedparser.FeedParserDict, feed_url: str, time_threshold: datetime
) -> list[dict]:
    results = []
    source = extract_source(feed_url)

    for entry in feed.entries:
        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()

        if not title or not link:
            continue

        pub_date = parse_date(entry)
        if pub_date <= time_threshold:
            continue

        summary_raw = entry.get("summary") or entry.get("description") or ""
        summary = clean_html(summary_raw)

        if not summary:
            summary = title

        assets = match_assets(title + " " + summary)
        if not assets:
            continue

        results.append(
            {
                "news_id": make_news_id(link, title),
                "title": title,
                "content": summary[:5000],
                "url": link,
                "source": source,
                "published_at": pub_date.isoformat(),
                "assets": assets,
            }
        )
    return results


def filter_existing(bq_client: bigquery.Client, entries: list[dict]) -> list[dict]:
    if not entries:
        return []

    ids = [e["news_id"] for e in entries if e.get("news_id")]
    if not ids:
        logger.warning("filter_existing: entries present but no valid news_id found")
        return []

    query = f"SELECT news_id FROM `{TABLE_ID}` WHERE news_id IN UNNEST(@ids)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", ids)]
    )
    existing = {
        row.news_id for row in bq_client.query(query, job_config=job_config).result()
    }
    fresh = [e for e in entries if e.get("news_id") not in existing]
    logger.info("Dedup: %d total → %d fresh", len(entries), len(fresh))
    return fresh


async def run_pipeline(
    time_threshold: datetime,
) -> tuple[list[dict], list[str]]:
    async with httpx.AsyncClient() as client:
        fetched_results = await gather_all_feeds(client, RSS_FEEDS)

        all_entries: list[dict] = []
        feed_errors: list[str] = []

        for url, feed in fetched_results:
            if feed is None:
                feed_errors.append(url)
                continue

            entries = parse_feed_entries(feed, url, time_threshold)

            if entries:
                asset_counts: dict[str, int] = {}
                for e in entries:
                    for a in e["assets"]:
                        asset_counts[a] = asset_counts.get(a, 0) + 1
                logger.info(
                    "[%s] %d entries — %s",
                    extract_source(url),
                    len(entries),
                    asset_counts,
                )

            all_entries.extend(entries)

        await enrich_entries_with_full_text(client, all_entries)

    return all_entries, feed_errors


@functions_framework.http
def fetch_news(request):
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    all_entries, feed_errors = asyncio.run(run_pipeline(time_threshold))

    if not all_entries:
        return (
            json.dumps(
                {
                    "status": "ok",
                    "inserted": 0,
                    "message": "No relevant news found",
                    "feed_errors": feed_errors,
                }
            ),
            200,
            {"Content-Type": "application/json"},
        )

    fresh_entries = filter_existing(bq_client, all_entries)

    if not fresh_entries:
        return (
            json.dumps(
                {
                    "status": "ok",
                    "inserted": 0,
                    "message": "All entries already in database",
                    "feed_errors": feed_errors,
                }
            ),
            200,
            {"Content-Type": "application/json"},
        )

    errors = bq_client.insert_rows_json(TABLE_ID, fresh_entries)
    if errors:
        for err in errors:
            logger.error("BQ insert error: %s", err)
        return (
            json.dumps({"status": "error", "details": errors}),
            500,
            {"Content-Type": "application/json"},
        )

    asset_totals: dict[str, int] = {}
    for e in fresh_entries:
        for a in e["assets"]:
            asset_totals[a] = asset_totals.get(a, 0) + 1

    logger.info("Inserted %d rows: %s", len(fresh_entries), asset_totals)
    return (
        json.dumps(
            {
                "status": "ok",
                "inserted": len(fresh_entries),
                "by_asset": asset_totals,
                "feed_errors": feed_errors,
            }
        ),
        200,
        {"Content-Type": "application/json"},
    )
