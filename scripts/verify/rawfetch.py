"""Send raw Kafka Fetch requests (API key 1, version 4) for one partition and decode the record batches returned.

No Kafka client library is involved: a TCP socket, the Fetch wire format and the RecordBatch v2 format, decoded
with the Python standard library only. For every batch in the response it reports the header fields (baseOffset,
lastOffsetDelta, record count, producer id, baseSequence, CRC check) and, per record, offsetDelta and the key, so
that the offsets the broker assigns can be checked against the batch header and against the table.

Usage: python3 rawfetch.py <host:port> <topic> <partition> <offset>[,<offset>...] [--walk N] [--records]
  --walk N   after each first fetch, fetch again N times at (last offset in the response + 1), as a consumer would
  --records  also print every record (offsetDelta, absolute offset, key)
One JSON line per fetch goes to stdout; a readable summary per batch goes to stderr.
"""

from __future__ import annotations

import argparse
import json
import logging
import socket
import struct
import sys

log = logging.getLogger("rawfetch")

ERRORS = {0: "NONE", 1: "OFFSET_OUT_OF_RANGE", 3: "UNKNOWN_TOPIC_OR_PARTITION", 6: "NOT_LEADER_OR_FOLLOWER"}


def _crc32c_table() -> list[int]:
    table = []
    for n in range(256):
        c = n
        for _ in range(8):
            c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
        table.append(c)
    return table


CRC32C = _crc32c_table()


def crc32c(data: bytes) -> int:
    c = 0xFFFFFFFF
    for b in data:
        c = CRC32C[(c ^ b) & 0xFF] ^ (c >> 8)
    return c ^ 0xFFFFFFFF


def varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Zigzag-encoded variable-length integer (Kafka record fields)."""
    shift = result = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return (result >> 1) ^ -(result & 1), pos
        shift += 7


def avro_string(raw: bytes | None) -> str | None:
    """Confluent-framed Avro string (magic 0, 4-byte schema id, zigzag length, UTF-8), else hex."""
    if raw is None:
        return None
    if len(raw) >= 6 and raw[0] == 0:
        length, pos = varint(raw, 5)
        return raw[pos:pos + length].decode("utf-8", "replace")
    return raw.hex()


def fetch_request(topic: str, partition: int, offset: int, max_bytes: int, correlation: int) -> bytes:
    t = topic.encode()
    client = b"rawfetch"
    header = struct.pack(">hhih", 1, 4, correlation, len(client)) + client
    body = struct.pack(">iiiib", -1, 5000, 1, max_bytes, 0)  # replica -1, max wait 5 s, min 1 byte, read uncommitted
    body += struct.pack(">ih", 1, len(t)) + t + struct.pack(">i", 1) + struct.pack(">iqi", partition, offset, max_bytes)
    req = header + body
    return struct.pack(">i", len(req)) + req


def recv_all(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed by broker")
        buf += chunk
    return buf


def parse_records(data: bytes, base_offset: int, count: int) -> list[dict]:
    records, pos = [], 0
    for _ in range(count):
        length, pos = varint(data, pos)
        end = pos + length
        pos += 1  # record attributes
        _ts_delta, pos = varint(data, pos)
        offset_delta, pos = varint(data, pos)
        key_len, pos = varint(data, pos)
        key = None if key_len < 0 else data[pos:pos + key_len]
        pos += max(key_len, 0)
        value_len, pos = varint(data, pos)
        records.append({"offset_delta": offset_delta, "offset": base_offset + offset_delta,
                        "key": avro_string(key), "value_bytes": value_len})
        pos = end
    return records


def parse_batches(records: bytes) -> list[dict]:
    batches, pos = [], 0
    while pos + 12 <= len(records):
        base_offset, batch_len = struct.unpack_from(">qi", records, pos)
        end = pos + 12 + batch_len
        if end > len(records):
            break  # a partial batch at the end of the response (max bytes reached)
        (_epoch, magic, crc, attributes, last_delta, _base_ts, _max_ts, producer_id, _producer_epoch, base_seq,
         count) = struct.unpack_from(">ibIhiqqqhii", records, pos + 12)
        crc_ok = crc32c(records[pos + 21:end]) == crc
        batch = {"base_offset": base_offset, "last_offset_delta": last_delta, "last_offset": base_offset + last_delta,
                 "record_count": count, "magic": magic, "compression": attributes & 0x07, "crc_ok": crc_ok,
                 "producer_id": producer_id, "base_sequence": base_seq, "bytes": 12 + batch_len}
        if magic == 2 and attributes & 0x07 == 0:
            recs = parse_records(records[pos + 61:end], base_offset, count)
            deltas = [r["offset_delta"] for r in recs]
            batch["offset_deltas_are_index"] = deltas == list(range(count))
            batch["max_offset_delta"] = max(deltas) if deltas else None
            batch["first_key"], batch["last_key"] = (recs[0]["key"], recs[-1]["key"]) if recs else (None, None)
            batch["records"] = recs
        batches.append(batch)
        pos = end
    return batches


def fetch(sock: socket.socket, topic: str, partition: int, offset: int, max_bytes: int, correlation: int) -> dict:
    sock.sendall(fetch_request(topic, partition, offset, max_bytes, correlation))
    size = struct.unpack(">i", recv_all(sock, 4))[0]
    resp = recv_all(sock, size)
    pos = 4 + 4  # correlation id, throttle time
    (_n_topics,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    (tlen,) = struct.unpack_from(">h", resp, pos)
    pos += 2 + tlen
    pos += 4  # partition count (1)
    part, err, high, _lso = struct.unpack_from(">ihqq", resp, pos)
    pos += 4 + 2 + 8 + 8
    (n_aborted,) = struct.unpack_from(">i", resp, pos)
    pos += 4 + max(n_aborted, 0) * 16
    (rlen,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    records = resp[pos:pos + rlen] if rlen > 0 else b""
    return {"fetch_offset": offset, "partition": part, "error": ERRORS.get(err, err), "high_watermark": high,
            "records_bytes": len(records), "batches": parse_batches(records)}


def summarize(result: dict, with_records: bool) -> None:
    log.info("fetch offset %d -> %s, high watermark %d, %d bytes, %d complete batches", result["fetch_offset"],
             result["error"], result["high_watermark"], result["records_bytes"], len(result["batches"]))
    for b in result["batches"]:
        log.info("  batch baseOffset %d lastOffsetDelta %d (last %d) count %d | producerId %d baseSequence %d | "
                 "crc %s | offsetDeltas == index: %s | keys %s .. %s", b["base_offset"], b["last_offset_delta"],
                 b["last_offset"], b["record_count"], b["producer_id"], b["base_sequence"],
                 "ok" if b["crc_ok"] else "BAD", b.get("offset_deltas_are_index"), b.get("first_key"),
                 b.get("last_key"))
        if with_records:
            for r in b.get("records", []):
                log.info("    offsetDelta %d offset %d key %s value %d bytes", r["offset_delta"], r["offset"], r["key"],
                         r["value_bytes"])


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("broker")
    p.add_argument("topic")
    p.add_argument("partition", type=int)
    p.add_argument("offsets")
    p.add_argument("--walk", type=int, default=0)
    p.add_argument("--max-bytes", type=int, default=1048576)
    p.add_argument("--records", action="store_true")
    args = p.parse_args(argv)
    host, port = args.broker.split(":")
    correlation = 0
    with socket.create_connection((host, int(port)), timeout=30) as sock:
        for start in (int(o) for o in args.offsets.split(",")):
            offset = start
            for _ in range(args.walk + 1):
                correlation += 1
                result = fetch(sock, args.topic, args.partition, offset, args.max_bytes, correlation)
                summarize(result, args.records)
                if not args.records:
                    for b in result["batches"]:
                        b.pop("records", None)
                print(json.dumps(result))  # machine-parsed stdout protocol
                if not result["batches"]:
                    break
                offset = result["batches"][-1]["last_offset"] + 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
