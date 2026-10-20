#!/usr/bin/env bash
# (iii) What the Iceberg REST catalog answers: namespaces, the table (first 60 lines), the snapshot summaries.
source "$(dirname "$0")/lib.sh"

note "namespaces"
echo "\$ curl -s ${REST}/v1/namespaces"
curl -s "${REST}/v1/namespaces"; echo; echo

note "catalog config"
echo "\$ curl -s ${REST}/v1/config"
curl -s "${REST}/v1/config"; echo; echo

note "the table, first 60 lines of the pretty-printed answer"
table_json="$(curl -s "${REST}/v1/namespaces/default/tables/events")"
echo "\$ curl -s ${REST}/v1/namespaces/default/tables/events | python3 -m json.tool | head -60"
pretty="$(python3 -m json.tool <<<"${table_json}")"
sed -n 1,60p <<<"${pretty}"  # the first 60 lines, without an early-exiting head (SIGPIPE under pipefail)
echo

note "metadata-location and snapshot summaries from the same answer"
echo "\$ curl -s ${REST}/v1/namespaces/default/tables/events | python3 (print metadata-location, current-snapshot-id, every snapshot summary)"
echo "${table_json}" | python3 -c '
import json, sys
t = json.load(sys.stdin)
meta = t["metadata"]
print("metadata-location:", t["metadata-location"])
print("table location:", meta["location"])
print("current-snapshot-id:", meta.get("current-snapshot-id"))
for s in meta.get("snapshots", []):
    summary = s["summary"]
    print("snapshot", s["snapshot-id"], summary.get("operation"),
          "added-records=" + str(summary.get("added-records")),
          "total-records=" + str(summary.get("total-records")),
          "added-data-files=" + str(summary.get("added-data-files")),
          "total-data-files=" + str(summary.get("total-data-files")),
          "manifest-list=" + s["manifest-list"])
'
echo
