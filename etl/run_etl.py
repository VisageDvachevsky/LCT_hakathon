# etl/run_etl.py
import os
import argparse
from dotenv import load_dotenv
from etl.utils.config import Settings
from etl.flows.ingest_from_files import flow_ingest_from_files
from etl.flows.parse_and_normalize import flow_parse_and_normalize
from etl.flows.enrich_features import flow_enrich_features
from etl.flows.load_to_core import flow_load_to_core
from etl.flows.publish_views import flow_publish_views
from etl.utils.logger import get_logger
from etl.utils.schema import ensure_schema
from etl.utils.db import init_db

log = get_logger(__name__)

STEP_ORDER = ["ingest", "parse", "enrich", "load", "publish"]

def _parse_steps(step_arg: str):
    if not step_arg:
        return STEP_ORDER
    parts = [p.strip().lower() for p in step_arg.split(",") if p.strip()]
    # validate
    invalid = [p for p in parts if p not in STEP_ORDER]
    if invalid:
        raise ValueError(f"Invalid steps requested: {invalid}. Allowed: {STEP_ORDER}")
    return parts

def main(argv=None):
    load_dotenv()
    s = Settings.from_env()
    ensure_schema(s)
    init_db(s)
    parser = argparse.ArgumentParser(prog="run_etl", description="Run ETL pipeline")
    parser.add_argument("--steps", type=str, default=",".join(STEP_ORDER),
                        help="Comma-separated steps to run: ingest,parse,enrich,load,publish (default all)")
    parser.add_argument("--load-id", type=str, default=None, help="Run pipeline only for this load_id (UUID string). If omitted, run for all ingested load_ids (if ingest step ran) or all found in quality_load_log.")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode: do not write to core tables (some steps may still write staged tables).")
    args = parser.parse_args(argv)

    try:
        steps = _parse_steps(args.steps)
    except Exception as e:
        log.error("Failed to parse steps: %s", e)
        return 2

    log.info("Starting ETL pipeline", extra={"steps": steps, "load_id": args.load_id, "dry_run": args.dry_run})

    load_ids = []
    try:
        if "ingest" in steps:
            # ingest returns list of load_id (strings)
            try:
                ids = flow_ingest_from_files(s)
                log.info("Ingest produced load_ids", extra={"count": len(ids), "ids": ids})
            except Exception:
                log.exception("Ingest step failed")
                ids = []
            if args.load_id:
                # if user provided load_id, use intersection if possible
                load_ids = [args.load_id] if args.load_id in ids else ([args.load_id] if args.load_id else ids)
            else:
                load_ids = ids
        else:
            # if no ingest step, but load-id provided — we run for that one only
            if args.load_id:
                load_ids = [args.load_id]
            else:
                # Try to find recent load_ids from quality_load_log if present (non-empty)
                # To avoid adding DB dependency here, we will default to empty list (user should pass --load-id or run ingest).
                load_ids = []
    except Exception:
        log.exception("Failed to determine load_ids to process")
        load_ids = []

    # If nothing to do but publish view, still allow publish
    if not load_ids and any(s in steps for s in ["parse", "enrich", "load"]):
        log.warning("No load_ids found for processing. Skipping parse/enrich/load steps.")
    # Process each load_id step-by-step
    for lid in load_ids:
        # parse
        if "parse" in steps:
            try:
                flow_parse_and_normalize(s, lid)
                log.info("parse completed", extra={"load_id": lid})
            except Exception:
                log.exception("parse failed", extra={"load_id": lid})
                # continue with next load_id
                continue

        if "enrich" in steps:
            try:
                flow_enrich_features(s, lid)
                log.info("enrich completed", extra={"load_id": lid})
            except Exception:
                log.exception("enrich failed", extra={"load_id": lid})
                continue

        if "load" in steps:
            if args.dry_run:
                log.info("dry-run: skipping load_to_core", extra={"load_id": lid})
            else:
                try:
                    flow_load_to_core(s, lid)
                    log.info("load_to_core completed", extra={"load_id": lid})
                except Exception:
                    log.exception("load_to_core failed", extra={"load_id": lid})
                    continue

    # publish step is global (not per-load_id)
    if "publish" in steps:
        try:
            flow_publish_views(s)
            log.info("publish_views completed")
        except Exception:
            log.exception("publish_views failed")

    log.info("ETL pipeline completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
