#!/usr/bin/env python3
"""Forward walk of the Bybit cold-wallet theft on Ethereum.

Starts at the cold wallet and the address that received the stolen funds.
Follows outgoing transfers inside 2025-02-21 00:00 UTC through
2025-03-07 23:59:59 UTC, for up to 10 hops. An address is not expanded
when its transaction count is over 10,000; the transfer into it is kept.
"""

import argparse
import csv
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

COLD = "0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4"
SEED = "0x47666fab8bd0ac7003bce3f5c3585383f09486e2"
STARTS = [COLD, SEED]
MAX_HOPS = 10
STOP_AT = 10000
NODES = Path("data/nodes.csv")
EDGES = Path("data/edges.csv")
EXPANDED = Path("data/expanded.txt")
LOG = Path("data/download.log")
API = "https://api.routescan.io/v2/network/mainnet/evm/1/etherscan/api"
WINDOW_START = datetime(2025, 2, 21, tzinfo=timezone.utc)
WINDOW_END = datetime(2025, 3, 7, 23, 59, 59, tzinfo=timezone.utc)
THEFT = datetime(2025, 2, 21, 14, 13, 35, tzinfo=timezone.utc)
EDGE_FIELDS = [
    "source",
    "target",
    "tx",
    "asset",
    "amount",
    "time",
    "block_number",
    "nonce",
    "gas_used",
    "gas_price",
    "method",
    "token_contract",
    "signer",
    "transfer_index",
]

def log(message):
    line = time.strftime("%H:%M:%S") + "  " + message
    print(line, flush=True)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as handle:
        handle.write(line + "\n")


parser = argparse.ArgumentParser(description="Download the theft wallet graph")
parser.add_argument(
    "--is-spam",
    action="store_true",
    help="also save the explorer scam flag (is_scam) on each wallet",
)
args = parser.parse_args()


def rate_limited(payload):
    if not isinstance(payload, dict):
        return False
    result = payload.get("result")
    text = " ".join(
        [
            str(payload.get("message") or ""),
            result if isinstance(result, str) else "",
        ]
    ).lower()
    return "rate limit" in text or "too many" in text


def call_json(url, timeout=60):
    for attempt in range(8):
        try:
            request = urllib.request.Request(url, headers={"accept": "application/json"})
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                payload = json.load(resp)
        except urllib.error.HTTPError as err:
            if err.code != 429 or attempt == 7:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        except (urllib.error.URLError, TimeoutError):
            if attempt == 7:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        if rate_limited(payload) and attempt < 7:
            time.sleep(5 * (attempt + 1))
            continue
        return payload


def block_by_time(when, closest):
    query = urllib.parse.urlencode(
        {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": int(when.timestamp()),
            "closest": closest,
        }
    )
    payload = call_json(API + "?" + query)
    if payload.get("message") != "OK":
        raise SystemExit(payload.get("result"))
    return int(payload["result"])


def fetch(action, address):
    # Page by block. page×offset past 10,000 is rejected by this API.
    rows = []
    start = start_block
    while start <= end_block:
        query = urllib.parse.urlencode(
            {
                "module": "account",
                "action": action,
                "address": address,
                "startblock": start,
                "endblock": end_block,
                "page": 1,
                "offset": 10000,
                "sort": "asc",
            }
        )
        payload = call_json(API + "?" + query, timeout=90)
        if payload.get("message") == "No transactions found":
            return rows
        if payload.get("message") != "OK":
            raise RuntimeError(payload.get("result"))
        batch = payload["result"]
        if not isinstance(batch, list) or not batch:
            return rows
        rows.extend(batch)
        if len(batch) < 10000:
            return rows
        next_start = int(batch[-1]["blockNumber"])
        start = next_start + 1 if next_start <= start else next_start
    return rows


def human(value, decimals):
    amount = int(value)
    scale = 10 ** decimals
    whole, frac = divmod(amount, scale)
    if frac == 0:
        return str(whole)
    return f"{whole}.{frac:0{decimals}d}".rstrip("0").rstrip(".")


signers = {}
signer_lock = threading.Lock()


def tx_signer(tx_hash):
    with signer_lock:
        if tx_hash in signers:
            return signers[tx_hash]
    query = urllib.parse.urlencode(
        {
            "module": "proxy",
            "action": "eth_getTransactionByHash",
            "txhash": tx_hash,
        }
    )
    payload = call_json(API + "?" + query)
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    signer = (result.get("from") or "").lower()
    with signer_lock:
        signers[tx_hash] = signer
    return signer


def transfer(row, asset, decimals, token_contract="", lookup_signer=False):
    if int(row.get("value") or 0) == 0 or row.get("isError") not in (None, "0"):
        return None
    if not row.get("from") or not row.get("to"):
        return None
    when = datetime.fromtimestamp(int(row["timeStamp"]), timezone.utc)
    if when < WINDOW_START or when > WINDOW_END:
        return None
    source = row["from"].lower()
    target = row["to"].lower()
    contract = (token_contract or "").lower()
    method = row.get("functionName") or ""
    if not method and row.get("methodId") not in (None, "", "0x"):
        method = row["methodId"]
    tx = row.get("hash") or row["transactionHash"]
    return {
        "source": source,
        "target": target,
        "tx": tx,
        "asset": asset,
        "amount": human(row["value"], decimals),
        "time": when.isoformat(),
        "block_number": row.get("blockNumber") or "",
        "nonce": row.get("nonce") or "",
        "gas_used": row.get("gasUsed") or "",
        "gas_price": row.get("gasPrice") or "",
        "method": method,
        "token_contract": contract,
        "signer": source,
        "transfer_index": str(row.get("logIndex") or row.get("traceId") or ""),
        "_lookup": lookup_signer,
    }


def keep(item, addr, cutoff):
    if not item or item["source"] != addr or item["time"] < cutoff:
        return None
    if addr == COLD and item["target"] != SEED:
        return None
    if item.pop("_lookup"):
        item["signer"] = tx_signer(item["tx"])
    return item


def outgoing(addr, cutoff):
    # Outgoing real transfers at or after the moment this address received funds.
    found = []
    for row in fetch("txlistinternal", addr):
        item = keep(transfer(row, "ETH", 18, lookup_signer=True), addr, cutoff)
        if item:
            found.append(item)
    for row in fetch("txlist", addr):
        item = keep(transfer(row, "ETH", 18), addr, cutoff)
        if item:
            found.append(item)
    for row in fetch("tokentx", addr):
        item = keep(
            transfer(
                row,
                row.get("tokenSymbol") or "TOKEN",
                int(row.get("tokenDecimal") or 0),
                token_contract=row.get("contractAddress") or "",
                lookup_signer=True,
            ),
            addr,
            cutoff,
        )
        if item:
            found.append(item)
    return found


def first_row(action, address):
    query = urllib.parse.urlencode(
        {
            "module": "account",
            "action": action,
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 1,
            "sort": "asc",
        }
    )
    payload = call_json(API + "?" + query)
    if payload.get("message") == "No transactions found":
        return None
    if payload.get("message") != "OK":
        raise RuntimeError(payload.get("result"))
    batch = payload["result"]
    return batch[0] if batch else None


def tag_names(tags):
    names = []
    for tag in tags or []:
        if isinstance(tag, str):
            names.append(tag)
        elif isinstance(tag, dict):
            names.append(tag.get("display_name") or tag.get("label") or "")
    return ";".join(name for name in names if name)


def address_info(addr):
    # One pass: earliest on-chain transfer, then the explorer record for this address.
    stamps = []
    for action in ("txlist", "txlistinternal", "tokentx"):
        row = first_row(action, addr)
        if row and row.get("timeStamp"):
            stamps.append(int(row["timeStamp"]))
    when = ""
    if stamps:
        when = datetime.fromtimestamp(min(stamps), timezone.utc).isoformat()
    data = call_json("https://eth.blockscout.com/api/v2/addresses/" + addr)
    counters = call_json("https://eth.blockscout.com/api/v2/addresses/" + addr + "/counters")
    if "transactions_count" not in counters:
        raise RuntimeError(counters)
    tx_count = int(counters["transactions_count"])
    token_count = int(counters.get("token_transfers_count") or 0)
    info = {
        "id": addr,
        "is_contract": data.get("is_contract"),
        "earliest_tx_time": when,
        "name": data.get("name") or "",
        "ens_domain_name": data.get("ens_domain_name") or "",
        "public_tags": tag_names(data.get("public_tags")),
        "transactions_count": str(max(tx_count, token_count)),
    }
    if args.is_spam:
        info["is_scam"] = data.get("is_scam")
    return info


def edge_key(item):
    return (
        item["tx"],
        item["source"],
        item["target"],
        item["asset"],
        item["token_contract"],
        item["amount"],
        item.get("transfer_index", ""),
    )


def remember(item):
    key = edge_key(item)
    if key in seen:
        return False
    seen.add(key)
    stored = {field: item.get(field, "") for field in EDGE_FIELDS}
    edges.append(stored)
    wallets.add(stored["source"])
    wallets.add(stored["target"])
    by_source.setdefault(stored["source"], []).append(stored)
    return True


def write_edges():
    EDGES.parent.mkdir(parents=True, exist_ok=True)
    tmp = EDGES.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EDGE_FIELDS)
        writer.writeheader()
        writer.writerows(edges)
    tmp.replace(EDGES)


def write_nodes():
    ordered = [saved[addr] for addr in sorted(wallets) if addr in saved]
    tmp = NODES.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ordered)
    tmp.replace(NODES)


def mark_expanded(addr):
    if addr in expanded:
        return
    expanded.add(addr)
    with EXPANDED.open("a") as f:
        f.write(addr + "\n")


def row_complete(row):
    if not row.get("earliest_tx_time") or row.get("is_contract") == "":
        return False
    if "public_tags" not in row or "transactions_count" not in row:
        return False
    if row.get("transactions_count") == "":
        return False
    if args.is_spam and "is_scam" not in row:
        return False
    return True


def load_edges():
    if not EDGES.exists():
        return
    rows = list(csv.DictReader(EDGES.open()))
    if not rows or "token_contract" not in rows[0] or "signer" not in rows[0]:
        log("old edges file has no token or signer column, starting fresh")
        return
    for row in rows:
        remember(row)


def load_saved():
    if not NODES.exists():
        return
    for row in csv.DictReader(NODES.open()):
        if row.get("id") and row_complete(row):
            saved[row["id"]] = row


def load_expanded():
    if not EXPANDED.exists() or not edges:
        return
    for line in EXPANDED.read_text().splitlines():
        addr = line.strip().lower()
        if addr:
            expanded.add(addr)


def ensure_nodes(addrs):
    pending = [addr for addr in addrs if addr not in saved]
    if not pending:
        return
    log(f"fetching details for {len(pending)} wallets")
    done = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(address_info, addr): addr for addr in pending}
        for future in as_completed(futures):
            addr = futures[future]
            done += 1
            try:
                info = future.result()
                saved[info["id"]] = info
            except Exception as err:
                log(f"could not fetch {addr}: {err}")
                continue
            if done % 25 == 0 or done == len(pending):
                write_nodes()
                log(f"fetched {done} of {len(pending)} wallets")
    write_nodes()


def expand_many(addrs):
    need = [addr for addr in addrs if addr not in expanded and addr in received]
    if not need:
        return
    log(f"following transfers out of {len(need)} wallets")
    done = 0
    finished = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(outgoing, addr, received[addr]): addr for addr in need}
        for future in as_completed(futures):
            addr = futures[future]
            done += 1
            try:
                items = future.result()
            except Exception as err:
                log(f"could not follow {addr}: {err}")
            else:
                for item in items:
                    remember(item)
                finished.append(addr)
            if done % 25 == 0 or done == len(need):
                write_edges()
                for done_addr in finished:
                    mark_expanded(done_addr)
                finished.clear()
                log(f"followed {done} of {len(need)} wallets")


def note_targets(addr):
    cutoff = received.get(addr, "")
    found = []
    for edge in by_source.get(addr, []):
        if edge["time"] < cutoff:
            continue
        dest = edge["target"]
        if dest not in received or edge["time"] < received[dest]:
            received[dest] = edge["time"]
        found.append(dest)
    return found


def busy(addr):
    return int(saved[addr]["transactions_count"]) > STOP_AT


edges = []
seen = set()
by_source = {}
wallets = set(STARTS)
saved = {}
expanded = set()
received = {}
fields = [
    "id",
    "is_contract",
    "earliest_tx_time",
    "name",
    "ens_domain_name",
    "public_tags",
    "transactions_count",
]
if args.is_spam:
    fields.append("is_scam")

start_block = block_by_time(WINDOW_START, "after")
end_block = block_by_time(WINDOW_END, "before")
load_edges()
load_saved()
load_expanded()
ensure_nodes(STARTS)

received[COLD] = THEFT.isoformat()
reached = set(STARTS)
frontier = []
for hop in range(1, MAX_HOPS + 1):
    edges_before = len(edges)
    reached_before = set(reached)
    if hop == 1:
        expand_many([COLD])
        note_targets(COLD)
        if SEED not in received:
            received[SEED] = WINDOW_START.isoformat()
        expand_many([SEED])
        frontier = [COLD, SEED]
    else:
        expand_many(frontier)
    for addr in frontier:
        for dest in note_targets(addr):
            reached.add(dest)
    new_addrs = [addr for addr in reached if addr not in reached_before]
    ensure_nodes(new_addrs)
    stopped = 0
    nxt = []
    for addr in new_addrs:
        if addr not in saved:
            continue
        if busy(addr):
            stopped += 1
            continue
        nxt.append(addr)
    write_nodes()
    write_edges()
    log(
        f"hop {hop}: {len(new_addrs)} new wallets, {len(edges) - edges_before} new transfers, {stopped} not followed (over {STOP_AT} transactions)"
    )
    if hop == MAX_HOPS or not nxt:
        break
    frontier = nxt

write_nodes()
write_edges()
log(f"saved {len(saved)} wallets to {NODES}")
log(f"saved {len(edges)} transfers to {EDGES}")
