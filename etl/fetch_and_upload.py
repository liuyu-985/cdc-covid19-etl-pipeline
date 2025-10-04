import os, json, requests
import logging
from google.cloud import storage
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import TimeSeries, Point
from google.protobuf.timestamp_pb2 import Timestamp


BATCH_SIZE = int(os.getenv("BATCH_SIZE",   50_000))
TOTAL_ROWS = int(os.getenv("TOTAL_ROWS", 106_219_500))
BUCKET     = os.getenv("RAW_BUCKET", "raw-data-landing")

def fetch_chunk(offset: int):
    url    = "https://data.cdc.gov/resource/vbim-akqf.json"
    params = {"$limit": BATCH_SIZE, "$offset": offset}
    resp   = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()

def upload_ndjson(records, gcs_path):
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob   = bucket.blob(gcs_path)
    
    lines  = "\n".join(json.dumps(r) for r in records)
    blob.upload_from_string(lines, content_type="application/json")
    print(f"Uploaded {len(records)} rows to gs://{BUCKET}/{gcs_path}")

def record_row_count_metric(n_rows, ds):
    client = monitoring_v3.MetricServiceClient()
    project_id   = os.getenv("GCP_PROJECT")
    project_name = f"projects/{project_id}"
    series = TimeSeries()
    series.metric.type = "custom.googleapis.com/covid/fetch_rows"
    series.resource.type = "global"
    series.metric.labels["partition_date"] = ds
    now = Timestamp()
    now.GetCurrentTime()
    point = Point({
        "interval": {"end_time": now},
        "value": {"int64_value": n_rows},
    })
    series.points = [point]
    try:
        client.create_time_series(name=project_name, time_series=[series])
        logging.info(f"Created time series custom.googleapis.com/covid/fetch_rows: project={project_id} partition_date={ds} rows={n_rows}")
    except Exception as e:
        logging.error(f"Failed to write metric for partition {ds}: {e}", exc_info=True)
        raise

def main(offset: int, ds: str, **kwargs):
    records = fetch_chunk(offset)
    path    = f"covid/parts/date={ds}/chunk={offset}.json"
    upload_ndjson(records, path)
    
    record_row_count_metric(len(records), ds)


if __name__ == "__main__":
    import sys
    main(int(sys.argv[1]), sys.argv[2])
