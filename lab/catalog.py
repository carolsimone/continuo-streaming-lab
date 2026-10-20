"""Minimal Iceberg REST catalog client: ask the catalog where a table's metadata lives."""

import json
import urllib.error
import urllib.request


class TableNotFound(Exception):
    pass


def table_metadata_location(rest_url: str, namespace: str, table: str, timeout: float = 5.0) -> str:
    url = f"{rest_url}/v1/namespaces/{namespace}/tables/{table}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.load(response)["metadata-location"]
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise TableNotFound(f"{namespace}.{table}") from exc
        raise
