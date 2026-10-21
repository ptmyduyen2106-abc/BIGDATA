"""Executed smoke check: local full-history import, no Kafka/streaming/model."""
from datetime import datetime, timezone
import json
from pathlib import Path

from phase3.streaming import create_spark
from phase3.schema import CLEAN_SCHEMA
from pipeline.history import import_history

evidence = Path("/app/outputs/evidence/batch-import-smoke.json")
run_root = Path("/app/outputs/runs/batch-import-check")
report = {"run_id": run_root.name, "status": "running", "started_at_utc": datetime.now(timezone.utc).isoformat()}
spark = None
try:
    if run_root.exists():
        raise FileExistsError(f"Refusing to overwrite existing smoke run: {run_root}")
    spark = create_spark("local[2]", enable_delta=False, kafka=False, download_jars=False)
    result = import_history(spark, "/app/data/sample/events_10000.csv", run_root.as_posix(), run_root.name)
    report["import"] = result
    assert result["raw_rows"] == 10000, result
    assert result["clean_rows"] == 9999, result
    assert result["rejected_rows"] == 0, result
    clean = spark.read.schema(CLEAN_SCHEMA).parquet((run_root / "clean").as_posix())
    quarantine = spark.read.parquet((run_root / "quarantine").as_posix())
    report["persisted_clean_rows"] = clean.count()
    report["persisted_distinct_event_ids"] = clean.select("event_id").distinct().count()
    report["persisted_quarantined_rows"] = quarantine.count()
    report["rows_with_kafka_metadata"] = clean.filter("kafka_topic IS NOT NULL OR kafka_offset IS NOT NULL").count()
    report["active_streaming_queries"] = len(spark.streams.active)
    assert report["persisted_clean_rows"] == 9999
    assert report["persisted_distinct_event_ids"] == 9999
    assert report["persisted_quarantined_rows"] == 0
    assert report["rows_with_kafka_metadata"] == 0
    assert report["active_streaming_queries"] == 0
    assert result["mode"] == "batch_full_history_no_watermark"
    report.update(status="passed", semantics="Actual sample batch import. No Kafka, streaming watermark, training or baseline-run mutation.")
except Exception as error:
    report.update(status="failed", error=f"{type(error).__name__}: {error}")
    raise
finally:
    report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if spark is not None:
        spark.stop()
