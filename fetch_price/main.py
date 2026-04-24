import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
import functions_framework
import httpx
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


TABLE_ID = os.environ.get("BQ_TABLE_ID", "your-dataset.prices")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")

ASSETS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
}

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"


bq_client = bigquery.Client()
http_client = httpx.Client(timeout=10.0)


@dataclass
class PricePoint:
    asset: str
    price: float
    volume_24h: float
    market_cap: float
    change_24h: float
    collected_at: str

    def to_bq_row(self) -> dict:
        return {
            "asset": self.asset,
            "price": self.price,
            "volume": self.volume_24h,
            "market_cap": self.market_cap,
            "change_24h": self.change_24h,
            "timestamp": self.collected_at,
        }


def fetch_prices(assets: dict[str, str], retries: int = 3) -> dict:
    ids = ",".join(assets.values())
    params = {
        "ids": ids,
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_market_cap": "true",
    }
    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-pro-api-key"] = COINGECKO_API_KEY

    for attempt in range(retries):
        try:
            resp = http_client.get(
                COINGECKO_URL,
                params=params,
                headers=headers,
            )

            if resp.status_code == 429:
                wait = 2**attempt
                logger.warning("Rate limited by CoinGecko, retrying in %ss...", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except httpx.TimeoutException:
            logger.error(
                "CoinGecko request timed out (attempt %d/%d)", attempt + 1, retries
            )
            if attempt == retries - 1:
                raise
    return {}


def parse_prices(
    raw: dict, assets: dict[str, str]
) -> tuple[list[PricePoint], list[str]]:
    collected_at = datetime.now(timezone.utc).isoformat()
    points = []
    errors = []

    for ticker, coin_id in assets.items():
        coin_data = raw.get(coin_id)

        if not coin_data:
            logger.warning("No data for %s (%s) in CoinGecko response", ticker, coin_id)
            errors.append(ticker)
            continue

        price = coin_data.get("usd")
        volume = coin_data.get("usd_24h_vol")
        change = coin_data.get("usd_24h_change")

        if price is None:
            logger.error("Price is None for %s — skipping", ticker)
            errors.append(ticker)
            continue

        market_cap = coin_data.get("usd_market_cap")

        points.append(
            PricePoint(
                asset=ticker,
                price=float(price),
                volume_24h=float(volume) if volume is not None else 0.0,
                market_cap=float(market_cap) if market_cap is not None else 0.0,
                change_24h=float(change) if change is not None else 0.0,
                collected_at=collected_at,
            )
        )

    return points, errors


def save_to_bigquery(points: list[PricePoint]) -> list[dict]:
    rows = [p.to_bq_row() for p in points]
    errors = bq_client.insert_rows_json(TABLE_ID, rows)

    if errors:
        for err in errors:
            logger.error("BigQuery insert error: %s", err)

    return errors


@functions_framework.http
def fetch_price(request):
    logger.info("Starting price collection for %d assets", len(ASSETS))

    try:
        raw = fetch_prices(ASSETS)
    except Exception as e:
        logger.exception("Failed to fetch prices from CoinGecko")
        return (
            json.dumps({"status": "error", "message": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )

    points, parse_errors = parse_prices(raw, ASSETS)

    if not points:
        msg = "No valid price data parsed"
        logger.error(msg)
        return (
            json.dumps({"status": "error", "message": msg}),
            500,
            {"Content-Type": "application/json"},
        )

    bq_errors = save_to_bigquery(points)

    result = {
        "status": "ok" if not bq_errors else "partial",
        "saved": len(points),
        "parse_errors": parse_errors,
        "bq_errors": len(bq_errors),
        "prices": {p.asset: p.price for p in points},
    }

    logger.info("Done: %s", result)
    status_code = 200 if not bq_errors else 207
    return json.dumps(result), status_code, {"Content-Type": "application/json"}
