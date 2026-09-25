#!/usr/bin/env python3
"""One-hop neighborhood of the wallet that received Bybit's cold-wallet funds."""

import argparse
import csv
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# This address received the ETH and tokens from Bybit's cold wallet.
SEED = "0x47666fab8bd0ac7003bce3f5c3585383f09486e2"
NODES = Path("data/nodes.csv")
EDGES = Path("data/edges.csv")
API = "https://api.routescan.io/v2/network/mainnet/evm/1/etherscan/api"

parser = argparse.ArgumentParser(description="Download the one-hop wallet graph")
parser.add_argument(
    "--is-spam",
    action="store_true",
    help="also save the explorer scam flag (is_scam) on each wallet",
)
args = parser.parse_args()


def fetch(action):
    rows = []
    page = 1
    while True:
        query = urllib.parse.urlencode(
            {
                "module": "account",
                "action": action,
                "address": SEED,
                "startblock": 0,
                "endblock": 99999999,
                "page": page,
                "offset": 10000,
                "sort": "asc",
            }
        )
        for attempt in range(8):
            try:
                with urllib.request.urlopen(API + "?" + query, timeout=90) as resp:
                    payload = json.load(resp)
                break
            except urllib.error.HTTPError as err:
                if err.code != 429 or attempt == 7:
                    raise
                time.sleep(5 * (attempt + 1))
        batch = payload["result"]
        if payload.get("message") == "No transactions found":
            return rows
        if payload.get("message") != "OK":
            raise SystemExit(batch)
        rows.extend(batch)
        if len(batch) < 10000:
            return rows
        page += 1


def human(value, decimals):
    amount = int(value)
    scale = 10 ** decimals
    whole, frac = divmod(amount, scale)
    if frac == 0:
        return str(whole)
    return f"{whole}.{frac:0{decimals}d}".rstrip("0").rstrip(".")


edges = []
seen = set()


def get_json(url):
    request = urllib.request.Request(url, headers={"accept": "application/json"})
    for attempt in range(8):
        try:
            with urllib.request.urlopen(request, timeout=60) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as err:
            if err.code != 429 or attempt == 7:
                raise
            time.sleep(5 * (attempt + 1))


signers = {}


def tx_signer(tx_hash):
    if tx_hash not in signers:
        query = urllib.parse.urlencode(
            {
                "module": "proxy",
                "action": "eth_getTransactionByHash",
                "txhash": tx_hash,
            }
        )
        payload = get_json(API + "?" + query)
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        signers[tx_hash] = (result.get("from") or "").lower()
    return signers[tx_hash]


def add(row, asset, decimals, token_contract="", lookup_signer=False):
    if int(row.get("value") or 0) == 0 or row.get("isError") not in (None, "0"):
        return
    if not row.get("from") or not row.get("to"):
        return
    method = row.get("functionName") or ""
    if not method and row.get("methodId") not in (None, "", "0x"):
        method = row["methodId"]
    tx = row.get("hash") or row["transactionHash"]
    item = {
        "source": row["from"].lower(),
        "target": row["to"].lower(),
        "tx": tx,
        "asset": asset,
        "amount": human(row["value"], decimals),
        "time": datetime.fromtimestamp(int(row["timeStamp"]), timezone.utc).isoformat(),
        "block_number": row.get("blockNumber") or "",
        "nonce": row.get("nonce") or "",
        "gas_used": row.get("gasUsed") or "",
        "gas_price": row.get("gasPrice") or "",
        "method": method,
        "token_contract": token_contract.lower(),
        "signer": tx_signer(tx) if lookup_signer else row["from"].lower(),
    }
    key = (item["tx"], item["source"], item["target"], item["asset"], item["amount"])
    if key not in seen:
        seen.add(key)
        edges.append(item)


for row in fetch("txlistinternal"):
    add(row, "ETH", 18, lookup_signer=True)
for row in fetch("txlist"):
    add(row, "ETH", 18)
for row in fetch("tokentx"):
    add(
        row,
        row.get("tokenSymbol") or "TOKEN",
        int(row.get("tokenDecimal") or 0),
        token_contract=row.get("contractAddress") or "",
        lookup_signer=True,
    )


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
    payload = get_json(API + "?" + query)
    if payload.get("message") == "No transactions found":
        return None
    if payload.get("message") != "OK":
        raise SystemExit(payload.get("result"))
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
    data = get_json("https://eth.blockscout.com/api/v2/addresses/" + addr)
    counters = get_json("https://eth.blockscout.com/api/v2/addresses/" + addr + "/counters")
    info = {
        "id": addr,
        "is_contract": data.get("is_contract"),
        "earliest_tx_time": when,
        "name": data.get("name") or "",
        "ens_domain_name": data.get("ens_domain_name") or "",
        "public_tags": tag_names(data.get("public_tags")),
        "transactions_count": counters.get("transactions_count") or "",
    }
    if args.is_spam:
        info["is_scam"] = data.get("is_scam")
    return info


wallets = sorted({side for row in edges for side in (row["source"], row["target"])})
wallet_ids = set(wallets)
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


saved = {}
if NODES.exists():
    for row in csv.DictReader(NODES.open()):
        if row.get("id") in wallet_ids and row_complete(row):
            saved[row["id"]] = row

pending = [wallet for wallet in wallets if wallet not in saved]
print(f"looking up {len(pending)} of {len(wallets)} wallets", flush=True)

NODES.parent.mkdir(parents=True, exist_ok=True)
with EDGES.open("w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
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
        ],
    )
    writer.writeheader()
    writer.writerows(edges)


def write_nodes():
    ordered = [saved[wallet] for wallet in wallets if wallet in saved]
    with NODES.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ordered)


def done_batch(i, total):
    return i % 25 == 0 or i == total


with ThreadPoolExecutor(max_workers=8) as pool:
    futures = {pool.submit(address_info, wallet): wallet for wallet in pending}
    for i, future in enumerate(as_completed(futures), start=1):
        info = future.result()
        saved[info["id"]] = info
        if done_batch(i, len(pending)):
            write_nodes()
            print(f"looked up {len(saved)} of {len(wallets)} wallets", flush=True)

write_nodes()
print(f"saved {len(saved)} wallets to {NODES}", flush=True)
print(f"saved {len(edges)} transfers to {EDGES}", flush=True)

