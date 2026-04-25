import asyncio
import bisect
import functions_framework
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from google.cloud import bigquery
import vertexai
from vertexai.generative_models import (
    GenerativeModel,
    GenerationConfig,
    HarmCategory,
    HarmBlockThreshold,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bq_client = bigquery.Client()

PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
LOCATION = os.environ.get("REGION", "europe-west1")
vertexai.init(project=PROJECT_ID, location=LOCATION)

BQ_NEWS_TABLE = os.environ["BQ_NEWS_TABLE"]
BQ_RESULTS_TABLE = os.environ["BQ_RESULTS_TABLE"]
BQ_ERRORS_TABLE = os.environ["BQ_ERRORS_TABLE"]
BQ_PRICE_TABLE = os.environ["BQ_PRICE_TABLE"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
LLM_CONCURRENCY = int(os.environ.get("LLM_CONCURRENCY", "10"))

response_schema = {
    "type": "OBJECT",
    "properties": {
        "results": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "asset": {"type": "STRING"},
                    "sentiment": {
                        "type": "STRING",
                        "enum": ["BULLISH", "BEARISH", "NEUTRAL"],
                    },
                    "score": {"type": "NUMBER"},
                    "explanation": {"type": "STRING"},
                    "confidence": {"type": "STRING", "enum": ["HIGH", "MEDIUM", "LOW"]},
                    "time_horizon": {
                        "type": "STRING",
                        "enum": ["IMMEDIATE", "SHORT", "LONG", "NONE"],
                    },
                    "already_priced_in": {"type": "BOOLEAN"},
                },
                "required": [
                    "asset",
                    "sentiment",
                    "score",
                    "explanation",
                    "confidence",
                    "time_horizon",
                    "already_priced_in",
                ],
            },
        }
    },
    "required": ["results"],
}

SYSTEM_PROMPT = """You are a professional crypto market analyst.

Score rules (-1.0 to +1.0):
  +0.8 to +1.0 — direct very positive (ETF approval, major partnership, regulatory win)
  +0.4 to +0.7 — moderately positive (growing adoption, positive signal)
  -0.1 to +0.3 — neutral or weakly related
  -0.4 to -0.7 — moderately negative (regulatory pressure, competition)
  -0.8 to -1.0 — direct very negative (hack, ban, exploit, major legal action)

Confidence:
  HIGH   — asset directly named or main subject
  MEDIUM — indirectly affected (competitor, sector-wide)
  LOW    — macro connection only (Fed rates, general sentiment)

Time horizon:
  IMMEDIATE — within hours (breaking: hack, ETF decision)
  SHORT     — 1-7 days (regulatory update, partnership)
  LONG      — weeks/months (protocol upgrade, macro shift)
  NONE      — no real price impact

Already priced in:
  true  — reaction >2% in <5min, or >1% in 10-20min (large caps)
  false — otherwise

Important: LOW confidence score must stay between -0.2 and +0.2.
Always include ALL listed assets in results."""

model = GenerativeModel(
    "gemini-2.5-flash",
    system_instruction=SYSTEM_PROMPT,
    generation_config=GenerationConfig(
        temperature=0.1,
        max_output_tokens=8192,
        response_mime_type="application/json",
        response_schema=response_schema,
    ),
    safety_settings={
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    },
)


def fetch_unanalyzed(limit: int) -> list[dict]:
    query = f"""
        SELECT news_id, title, content, assets, published_at
        FROM `{BQ_NEWS_TABLE}` n
        WHERE NOT EXISTS (
            SELECT 1 FROM `{BQ_RESULTS_TABLE}` r
            WHERE r.news_id = n.news_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM `{BQ_ERRORS_TABLE}` e
            WHERE e.news_id = n.news_id
              AND e.retry_count >= {MAX_RETRIES}
        )
        ORDER BY n.published_at DESC
        LIMIT {limit}
    """
    return [dict(row) for row in bq_client.query(query).result()]


def fetch_prices_before_batch(
    news_items: list[dict],
) -> dict[str, dict[str, dict]]:
    if not news_items:
        return {}

    all_times = [
        _ensure_utc(item["published_at"])
        for item in news_items
        if item.get("published_at")
    ]
    if not all_times:
        return {}

    range_start = min(all_times) - timedelta(minutes=5)
    range_end = max(all_times)

    all_assets = list({a for item in news_items for a in (item.get("assets") or [])})
    if not all_assets:
        return {}

    query = f"""
        SELECT asset, price, change_24h, timestamp
        FROM `{BQ_PRICE_TABLE}`
        WHERE asset IN UNNEST(@assets)
          AND timestamp BETWEEN @start AND @end
        ORDER BY asset, timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("assets", "STRING", all_assets),
            bigquery.ScalarQueryParameter(
                "start", "TIMESTAMP", range_start.isoformat()
            ),
            bigquery.ScalarQueryParameter("end", "TIMESTAMP", range_end.isoformat()),
        ]
    )

    price_timeline: dict[str, list[tuple]] = defaultdict(list)
    try:
        for row in bq_client.query(query, job_config=job_config).result():
            ts = _ensure_utc(row.timestamp)
            price_timeline[row.asset].append((ts, row.price, row.change_24h))
    except Exception as e:
        logger.error("Failed to fetch price timeline: %s", e)
        return {}

    result: dict[str, dict[str, dict]] = {}
    for item in news_items:
        news_id = item["news_id"]
        pub_time = _ensure_utc(item["published_at"])
        assets = list(item.get("assets") or [])
        result[news_id] = {}

        for asset in assets:
            timeline = price_timeline.get(asset)
            if not timeline:
                continue
            timestamps = [entry[0] for entry in timeline]
            idx = bisect.bisect_right(timestamps, pub_time) - 1
            if idx >= 0:
                _, price, change_24h = timeline[idx]
                result[news_id][asset] = {"price": price, "change_24h": change_24h}

    return result


def fetch_smoothed_prices_now(assets: list[str]) -> dict[str, dict]:
    if not assets:
        return {}

    query = f"""
        SELECT asset, AVG(price) AS avg_price, MAX(change_24h) AS change_24h
        FROM `{BQ_PRICE_TABLE}`
        WHERE asset IN UNNEST(@assets)
          AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
        GROUP BY asset
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("assets", "STRING", assets)]
    )
    result = {}
    try:
        for row in bq_client.query(query, job_config=job_config).result():
            result[row.asset] = {"price": row.avg_price, "change_24h": row.change_24h}
    except Exception as e:
        logger.error("Failed to fetch smoothed prices now: %s", e)
    return result


def _ensure_utc(dt) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def format_price_context(
    prices_before: dict[str, dict],
    prices_now: dict[str, dict],
    assets: list[str],
    minutes_elapsed: int,
) -> str:
    lines = []
    for asset in assets:
        p0 = prices_before.get(asset, {}).get("price")
        p1 = prices_now.get(asset, {}).get("price")
        if p0 and p1:
            reaction_pct = (p1 - p0) / p0 * 100
            lines.append(
                f"- {asset}: before=${p0:,.2f} | now(3m TWAP)=${p1:,.2f} | "
                f"reaction={reaction_pct:+.2f}% over {minutes_elapsed}min"
            )
        else:
            lines.append(f"- {asset}: price data unavailable")
    return "\n".join(lines)


def build_prompt(
    title: str, content: str, assets: list[str], price_context: str
) -> str:
    assets_str = ", ".join(assets)
    return f"""Analyze the news and assess its impact on each asset listed below.

MARKET REACTION (price right before news vs smoothed price right now):
{price_context}

Assets to analyze: {assets_str}

Title: {title}
Content: {content[:5000]}"""


async def analyze_news_async(
    title: str,
    content: str,
    assets: list[str],
    price_context: str,
    semaphore: asyncio.Semaphore,
    retries: int = MAX_RETRIES,
) -> list[dict] | None:
    prompt = build_prompt(title, content, assets, price_context)
    loop = asyncio.get_running_loop()

    for attempt in range(retries):
        try:
            async with semaphore:
                response = await loop.run_in_executor(
                    None,
                    lambda: model.generate_content(prompt),
                )

            if not response.candidates:
                logger.warning(
                    "No candidates (attempt %d/%d): %s",
                    attempt + 1,
                    retries,
                    title[:60],
                )
                return None

            try:
                raw = response.text.strip()
            except ValueError:
                finish_reason = response.candidates[0].finish_reason
                logger.warning(
                    "Safety block (finish_reason=%s): %s", finish_reason, title[:60]
                )
                return None

            results = json.loads(raw).get("results", [])
            if not results:
                await asyncio.sleep(2 ** (attempt + 1))
                continue

            valid = []
            for item in results:
                if item.get("asset") not in assets:
                    continue
                if item.get("sentiment") not in ("BULLISH", "BEARISH", "NEUTRAL"):
                    continue
                item.setdefault("time_horizon", "NONE")
                item.setdefault("already_priced_in", False)
                if item["time_horizon"] not in ("IMMEDIATE", "SHORT", "LONG", "NONE"):
                    item["time_horizon"] = "NONE"
                valid.append(item)

            return valid if valid else None

        except json.JSONDecodeError:
            await asyncio.sleep(2 ** (attempt + 1))
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "quota" in err or "resource_exhausted" in err:
                await asyncio.sleep(30 * (attempt + 1))
            elif "503" in err or "unavailable" in err or "overloaded" in err:
                await asyncio.sleep(2 ** (attempt + 1))
            else:
                logger.error("Unexpected error: %s", e)
                return None

    return None


def save_results(rows: list[dict]) -> list:
    if not rows:
        return []
    errors = bq_client.insert_rows_json(BQ_RESULTS_TABLE, rows)
    if errors:
        for err in errors:
            logger.error("BQ insert error: %s", err)
    return errors


def log_error(news_id: str, reason: str, force_max_retries: bool = False) -> None:
    increment = f"{MAX_RETRIES}" if force_max_retries else "T.retry_count + 1"
    initial_count = f"{MAX_RETRIES}" if force_max_retries else "1"

    query = f"""
        MERGE `{BQ_ERRORS_TABLE}` T
        USING (SELECT @news_id AS news_id, @reason AS reason, CURRENT_TIMESTAMP() AS ts) S
        ON T.news_id = S.news_id
        WHEN MATCHED THEN
            UPDATE SET
                retry_count = {increment},
                last_error  = S.reason,
                updated_at  = S.ts
        WHEN NOT MATCHED THEN
            INSERT (news_id, last_error, reason, retry_count, created_at, updated_at)
            VALUES (S.news_id, S.reason, S.reason, {initial_count}, S.ts, S.ts)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("news_id", "STRING", news_id),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        logger.error("Failed to log error for %s: %s", news_id, e)


async def _run_analysis(news_items: list[dict]) -> dict:
    now = datetime.now(timezone.utc)

    all_assets = list({a for item in news_items for a in (item.get("assets") or [])})
    prices_before_batch = fetch_prices_before_batch(news_items)
    prices_now = fetch_smoothed_prices_now(all_assets)

    logger.info(
        "Prices loaded — before: %d news items, now: %d assets",
        len(prices_before_batch),
        len(prices_now),
    )

    semaphore = asyncio.Semaphore(LLM_CONCURRENCY)
    tasks = []
    meta = []

    for item in news_items:
        assets = list(item.get("assets") or [])
        news_id = item["news_id"]

        if not assets:
            tasks.append(None)
            meta.append(None)
            continue

        pub_time = _ensure_utc(item.get("published_at"))
        minutes_elapsed = (
            max(0, int((now - pub_time).total_seconds() / 60)) if pub_time else 0
        )
        prices_before = prices_before_batch.get(news_id, {})
        price_context = format_price_context(
            prices_before, prices_now, assets, minutes_elapsed
        )

        tasks.append(
            analyze_news_async(
                title=item["title"],
                content=item["content"] or "",
                assets=assets,
                price_context=price_context,
                semaphore=semaphore,
            )
        )
        meta.append(
            {
                "news_id": news_id,
                "assets": assets,
                "minutes_elapsed": minutes_elapsed,
                "prices_before": prices_before,
            }
        )

    real_tasks = [t for t in tasks if t is not None]
    real_indices = [i for i, t in enumerate(tasks) if t is not None]

    analysis_results_raw = await asyncio.gather(*real_tasks, return_exceptions=True)

    results = []
    failed = 0
    skipped = sum(1 for t in tasks if t is None)

    for idx, analysis_list in zip(real_indices, analysis_results_raw):
        m = meta[idx]
        news_id = m["news_id"]
        minutes_elapsed = m["minutes_elapsed"]
        prices_before = m["prices_before"]

        if isinstance(analysis_list, Exception):
            logger.error("Task raised exception for %s: %s", news_id, analysis_list)
            failed += 1
            log_error(news_id, f"Exception: {analysis_list}")
            continue

        if analysis_list is None:
            failed += 1
            log_error(news_id, "LLM returned None after retries")
            continue

        has_valid_result = False
        for analysis in analysis_list:
            is_low_signal = (
                analysis["confidence"] == "LOW"
                and abs(float(analysis.get("score", 0))) < 0.2
            )
            if is_low_signal:
                log_error(
                    news_id,
                    f"Asset {analysis['asset']} filtered as low signal",
                    force_max_retries=True,
                )
                skipped += 1
                continue

            results.append(
                {
                    "news_id": news_id,
                    "asset": analysis["asset"],
                    "sentiment": analysis["sentiment"],
                    "score": float(analysis["score"]),
                    "explanation": str(analysis["explanation"])[:500],
                    "confidence": analysis["confidence"],
                    "time_horizon": analysis["time_horizon"],
                    "already_priced_in": analysis["already_priced_in"],
                    "price_before_news": prices_before.get(analysis["asset"], {}).get(
                        "price"
                    ),
                    "price_at_analysis": prices_now.get(analysis["asset"], {}).get(
                        "price"
                    ),
                    "change_24h_at_analysis": prices_now.get(analysis["asset"], {}).get(
                        "change_24h"
                    ),
                    "minutes_elapsed": minutes_elapsed,
                    "created_at": now.isoformat(),
                }
            )
            has_valid_result = True

        if not has_valid_result and analysis_list:
            logger.info("news_id=%s: all assets filtered as low signal", news_id)

    bq_errors = save_results(results)

    stats: dict[str, dict[str, int]] = {}
    for r in results:
        stats.setdefault(r["asset"], {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0})
        stats[r["asset"]][r["sentiment"]] += 1

    logger.info(
        "Done — inserted=%d failed=%d skipped=%d bq_errors=%d stats=%s",
        len(results),
        failed,
        skipped,
        len(bq_errors),
        stats,
    )

    return {
        "status": "ok" if not bq_errors else "partial",
        "analyzed": len(news_items),
        "inserted": len(results),
        "failed": failed,
        "skipped_low_signal": skipped,
        "by_asset": stats,
    }


@functions_framework.http
def analyze_news_handler(request):
    news_items = fetch_unanalyzed(limit=BATCH_SIZE)
    if not news_items:
        return (
            json.dumps(
                {"status": "ok", "analyzed": 0, "message": "Nothing to analyze"}
            ),
            200,
            {"Content-Type": "application/json"},
        )

    logger.info("Analyzing %d items", len(news_items))

    result = asyncio.run(_run_analysis(news_items))

    return json.dumps(result), 200, {"Content-Type": "application/json"}
