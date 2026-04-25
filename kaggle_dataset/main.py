import os
import json
import uuid
import shutil
import time
import logging
import functions_framework
from google.cloud import bigquery, storage
from google.api_core.exceptions import GoogleAPIError
from kaggle.api.kaggle_api_extended import KaggleApi

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = os.environ["DATASET_ID"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
KAGGLE_DATASET = os.environ["KAGGLE_DATASET"]
EXPORT_FORMAT = os.environ.get("EXPORT_FORMAT", "PARQUET").upper()
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))

TABLES_TO_EXPORT = [
    t.strip()
    for t in os.environ.get(
        "TABLES_TO_EXPORT", "analysis_results,prices,raw_news"
    ).split(",")
]

_bq_client = None
_storage_client = None
_kaggle_api = None


def _get_clients():
    global _bq_client, _storage_client, _kaggle_api
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
        log.info("BigQuery client initialised")
    if _storage_client is None:
        _storage_client = storage.Client(project=PROJECT_ID)
        log.info("Storage client initialised")
    if _kaggle_api is None:
        _kaggle_api = KaggleApi()
        _kaggle_api.authenticate()
        log.info("Kaggle API authenticated")
    return _bq_client, _storage_client, _kaggle_api


def _retry(fn, retries: int = MAX_RETRIES, delay: float = 5.0):
    """Linear-backoff retry wrapper for BigQuery / GCS operations."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except GoogleAPIError as exc:
            last_exc = exc
            log.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(delay * attempt)
    raise last_exc


def _gcs_prefix(run_id: str, table: str) -> str:
    return f"export_{run_id}/{table}/"


def _cleanup_gcs(bucket, run_id: str, tables: list[str]) -> None:
    """Delete temporary GCS files for all exported tables."""
    for table in tables:
        prefix = _gcs_prefix(run_id, table)
        blobs = list(bucket.list_blobs(prefix=prefix))
        if blobs:
            bucket.delete_blobs(blobs)
            log.info("GCS: deleted %d file(s) from %s", len(blobs), prefix)


def _cleanup_local(export_dir: str) -> None:
    """Remove the local export directory and all its contents."""
    if os.path.exists(export_dir):
        shutil.rmtree(export_dir)
        log.info("Local directory removed: %s", export_dir)


def _export_table(
    bq_client, storage_client, bucket, run_id: str, table_name: str, export_dir: str
) -> None:
    ext = ".parquet" if EXPORT_FORMAT == "PARQUET" else ".csv"
    destination_uri = f"gs://{BUCKET_NAME}/{_gcs_prefix(run_id, table_name)}{table_name}_*.{ext.lstrip('.')}"

    log.info("[%s] Exporting to GCS → %s", table_name, destination_uri)

    if table_name == "raw_news":
        query = f"""
            EXPORT DATA OPTIONS(
                uri='{destination_uri}',
                format='{EXPORT_FORMAT}'
            ) AS
            SELECT news_id, title, url, source, published_at, assets
            FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        """
    else:
        query = f"""
            EXPORT DATA OPTIONS(
                uri='{destination_uri}',
                format='{EXPORT_FORMAT}'
            ) AS
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        """

    _retry(lambda: bq_client.query(query, location=BQ_LOCATION).result())

    blobs = list(bucket.list_blobs(prefix=_gcs_prefix(run_id, table_name)))
    data_blobs = [b for b in blobs if b.name.endswith(ext)]

    if not data_blobs:
        raise ValueError(f"GCS: no files found for table '{table_name}'")

    for i, blob in enumerate(data_blobs):
        local_name = (
            f"{table_name}{ext}" if len(data_blobs) == 1 else f"{table_name}_{i}{ext}"
        )
        local_path = os.path.join(export_dir, local_name)
        _retry(lambda b=blob, p=local_path: b.download_to_filename(p))
        log.info("[%s] Downloaded: %s", table_name, local_name)


def _push_to_kaggle(kaggle_api, export_dir: str) -> None:
    slug = KAGGLE_DATASET.split("/")[1]
    title = slug.replace("-", " ").title()

    metadata = {
        "title": title,
        "id": KAGGLE_DATASET,
        "licenses": [{"name": "CC0-1.0"}],
    }
    with open(os.path.join(export_dir, "dataset-metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    try:
        kaggle_api.dataset_create_version(
            export_dir,
            version_notes="Automated data update via GCP pipeline",
            dir_mode="zip",
        )
        log.info("Kaggle: dataset version updated")
    except Exception as exc:
        if "404" in str(exc) or "not found" in str(exc).lower():
            log.warning("Dataset not found — creating a new one")
            kaggle_api.dataset_create_new(export_dir, dir_mode="zip", public=True)
            log.info("Kaggle: new dataset created")
        else:
            raise


# ── HTTP handler ─────────────────────────────────────────────────────────────


@functions_framework.http
def export_bq_to_kaggle(request):
    if request.path == "/favicon.ico":
        return ("", 204)

    run_id = uuid.uuid4().hex[:10]
    export_dir = f"/tmp/kaggle_export_{run_id}"
    log.info("Pipeline #%s started | project: %s", run_id, PROJECT_ID)

    bq_client, storage_client, kaggle_api = _get_clients()
    bucket = storage_client.bucket(BUCKET_NAME)

    try:
        os.makedirs(export_dir, exist_ok=True)

        for table_name in TABLES_TO_EXPORT:
            log.info("── Processing table: %s ──", table_name)
            _export_table(
                bq_client, storage_client, bucket, run_id, table_name, export_dir
            )

        _push_to_kaggle(kaggle_api, export_dir)
        log.info("Pipeline #%s completed successfully", run_id)
        return ("OK", 200)

    except Exception as exc:
        log.exception("Pipeline #%s failed: %s", run_id, exc)
        return (f"Error: {exc}", 500)

    finally:
        _cleanup_gcs(bucket, run_id, TABLES_TO_EXPORT)
        _cleanup_local(export_dir)
