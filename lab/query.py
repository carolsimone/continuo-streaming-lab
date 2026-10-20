"""Query the broker-written Iceberg table with DuckDB.

The catalog only answers "where is the table's current metadata file?". DuckDB then follows
metadata -> manifests -> Parquet files directly in MinIO.
"""

import argparse
import json
import logging
import sys

import duckdb

from lab import endpoints
from lab.catalog import TableNotFound, table_metadata_location

log = logging.getLogger(__name__)

DEFAULT_SQL = (
    'SELECT count(*) AS row_count, count(DISTINCT kafka.partition) AS partitions, '
    'min(kafka."offset") AS min_offset, max(kafka."offset") AS max_offset FROM {table}'
)


def render_sql(sql: str, metadata_location: str) -> str:
    return sql.replace("{table}", f"iceberg_scan('{metadata_location}')")


def connect(minio_endpoint: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    con.execute(
        "CREATE SECRET lab_minio (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', "
        f"ENDPOINT '{minio_endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')"
    )
    return con


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--namespace", default="default")
    p.add_argument("--table", default="events")
    p.add_argument("--rest", default=endpoints.catalog_url())
    p.add_argument("--minio", default=endpoints.minio_endpoint())
    p.add_argument("--sql", default=DEFAULT_SQL)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    # force=True: bind to the current sys.stderr even if the root logger already has handlers.
    logging.basicConfig(
        stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True
    )
    args = parse_args(argv)
    try:
        location = table_metadata_location(args.rest, args.namespace, args.table)
    except TableNotFound:
        log.error(
            "table %s.%s does not exist yet: no segment has been offloaded yet "
            "(produce more data or lower segment.bytes/segment.ms)",
            args.namespace, args.table,
        )
        return 3
    except Exception as exc:
        log.error("cannot reach the catalog at %s: %s", args.rest, exc)
        return 1
    log.info("metadata location: %s", location)
    try:
        cursor = connect(args.minio).execute(render_sql(args.sql, location))
        columns = [c[0] for c in cursor.description]
        for row in cursor.fetchall():
            print(json.dumps(dict(zip(columns, row)), default=str))  # machine-parsed stdout protocol
    except Exception as exc:
        log.error("query failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
