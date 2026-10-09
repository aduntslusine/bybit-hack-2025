"""Download the theft graph from BigQuery into out/.

One scan of 21 Feb–7 Mar 2025, then a local walk from the cold wallet.
Stops before a query would pass the free-tier cap.

  python3 download_graph.py --project YOUR_PROJECT_ID
  python3 download_graph.py --noise
"""

import argparse
import csv
import json
import random
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
DATASET = "bigquery-public-data.goog_blockchain_ethereum_mainnet_us"
TOKEN_TABLE = "bigquery-public-data.crypto_ethereum.tokens"

COLD = "0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4"
SEED = "0x47666fab8bd0ac7003bce3f5c3585383f09486e2"
START = "2025-02-21"
END = "2025-03-08"  # exclusive, so 7 Mar is included
PRIOR_START = "2024-11-23"  # 90 days before START
THEFT = "2025-02-21T14:13:35+00:00"
WINDOW = "2025-02-21T00:00:00+00:00"
HOPS = 10
BUDGET = 900 * 1024**3
# Counted 1:1 with ETH: native, stETH, mETH, cmETH, WETH.
NOISE_ASSETS = (
    "",
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84",
    "0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa",
    "0xe6829d9a7ee3040e1276fa75293bde931859e8fa",
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
)
NOISE_MIN = str(100 * 10**18)
NOISE_SEED_SHARE = 0.05

EDGE_FIELDS = [
    "source", "target", "tx", "asset", "amount", "time", "block_number",
    "nonce", "gas_used", "gas_price", "method", "token_contract", "signer", "transfer_index",
]
NODE_FIELDS = [
    "id", "is_contract", "hop", "first_received_time", "graph_in_count", "graph_out_count",
    "active_before", "tx_count_before", "last_seen_before",
]

LOG = None


def log(message):
    line = datetime.now().strftime("%H:%M:%S") + "  " + message
    print(line, flush=True)
    if LOG is not None:
        with LOG.open("a") as handle:
            handle.write(line + "\n")


def norm_time(value):
    if not isinstance(value, datetime):
        text = str(value).strip().replace(" ", "T").replace("Z", "+00:00")
        if text.endswith(" UTC"):
            text = text[:-4] + "+00:00"
        value = datetime.fromisoformat(text)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def cell(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return norm_time(value)
    return str(value)


def queries():
    def window(alias=""):
        prefix = f"{alias}." if alias else ""
        return f"{prefix}block_timestamp >= '{START}' AND {prefix}block_timestamp < '{END}'"

    when = window()
    # value_lossless keeps the full amount. Trace amounts live under action, not on the row.
    # input is read only for the 4-byte method. Call data and trace output are not read.
    return {
        "transactions": f"""
            SELECT t.transaction_hash AS `hash`, t.from_address AS signer,
                   CAST(r.gas_used AS STRING) AS gas_used,
                   CAST(r.effective_gas_price AS STRING) AS gas_price,
                   CAST(t.nonce AS STRING) AS nonce,
                   IF(STARTS_WITH(IFNULL(t.input, ''), '0x') AND LENGTH(t.input) >= 10, SUBSTR(t.input, 1, 10), '') AS method,
                   t.to_address, t.value_lossless AS value,
                   t.block_timestamp, CAST(t.block_number AS STRING) AS block_number
            FROM `{DATASET}.transactions` AS t
            JOIN `{DATASET}.receipts` AS r
              ON r.transaction_hash = t.transaction_hash
             AND r.block_timestamp = t.block_timestamp
            WHERE {window("t")} AND {window("r")} AND r.status = 1 AND t.from_address IS NOT NULL
        """,
        "traces": f"""
            SELECT action.from_address AS source, action.to_address AS target,
                   transaction_hash AS `hash`, action.value_lossless AS value,
                   block_timestamp, CAST(block_number AS STRING) AS block_number,
                   (SELECT STRING_AGG(CAST(item AS STRING), ',') FROM UNNEST(trace_address) AS item) AS transfer_index
            FROM `{DATASET}.traces`
            WHERE {when}
              AND action.from_address IS NOT NULL AND action.to_address IS NOT NULL
              AND SAFE_CAST(action.value_lossless AS BIGNUMERIC) > 0
              AND ARRAY_LENGTH(trace_address) > 0
              AND trace_type IN ('call', 'create')
              AND (action.call_type IS NULL OR action.call_type NOT IN ('delegatecall', 'staticcall'))
              AND error IS NULL
        """,
        "token_transfers": f"""
            SELECT from_address AS source, to_address AS target, transaction_hash AS `hash`,
                   quantity AS value, block_timestamp, CAST(block_number AS STRING) AS block_number,
                   LOWER(address) AS token_contract, CAST(event_index AS STRING) AS transfer_index
            FROM `{DATASET}.token_transfers`
            WHERE {when}
              AND from_address IS NOT NULL AND to_address IS NOT NULL
              AND SAFE_CAST(quantity AS BIGNUMERIC) > 0
              AND token_id IS NULL
              AND (removed IS NULL OR removed = FALSE)
        """,
    }


def load_state():
    path = OUT / "state.json"
    if not path.exists():
        return {"spent": 0}
    return json.loads(path.read_text())


def save_state(state):
    (OUT / "state.json").write_text(json.dumps(state) + "\n")


def download_table(client, name, sql, budget, state):
    dest = OUT / "raw" / f"{name}.csv"
    if dest.exists() and dest.stat().st_size > 0:
        log(f"{name} already saved")
        return
    from google.cloud import bigquery

    estimate = int(client.query(
        sql, location="US",
        job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
    ).total_bytes_processed or 0)
    left = budget - state["spent"]
    log(f"{name} {estimate // 1024**3} GiB")
    if estimate > left:
        log("over the cap")
        raise SystemExit(2)
    job = client.query(sql, location="US", job_config=bigquery.QueryJobConfig(maximum_bytes_billed=int(left)))
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(".csv.partial")
    count = 0
    with partial.open("w", newline="") as handle:
        writer = None
        for row in job.result(page_size=20000):
            if writer is None:
                writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
                writer.writeheader()
            writer.writerow({key: cell(row[key]) for key in writer.fieldnames})
            count += 1
            if count % 250000 == 0:
                log(f"{name} {count}")
    partial.replace(dest)
    state["spent"] += int(job.total_bytes_billed or 0)
    save_state(state)
    log(f"{name} {count} rows")


def connect(path):
    con = sqlite3.connect(path, timeout=300)
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS tx (
            hash TEXT PRIMARY KEY, signer TEXT, nonce TEXT, gas_used TEXT, gas_price TEXT, method TEXT
        );
        CREATE TABLE IF NOT EXISTS xfer (
            source TEXT, target TEXT, hash TEXT, value TEXT, time TEXT,
            block_number TEXT, token_contract TEXT, transfer_index TEXT
        );
        CREATE TABLE IF NOT EXISTS reached (address TEXT PRIMARY KEY, first_time TEXT, hop INTEGER);
        CREATE TABLE IF NOT EXISTS expanded (address TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS edge (
            id INTEGER PRIMARY KEY, source TEXT, target TEXT, tx TEXT, time TEXT,
            block_number TEXT, token_contract TEXT, transfer_index TEXT, value TEXT
        );
        CREATE TABLE IF NOT EXISTS busy (address TEXT PRIMARY KEY, n INTEGER);
        CREATE TABLE IF NOT EXISTS account (address TEXT PRIMARY KEY, is_contract TEXT);
        CREATE TABLE IF NOT EXISTS token (address TEXT PRIMARY KEY, symbol TEXT, name TEXT, decimals TEXT);
        CREATE TABLE IF NOT EXISTS prior (address TEXT PRIMARY KEY, n INTEGER NOT NULL, last_seen TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS prior_done (k TEXT PRIMARY KEY);
        """
    )
    return con


def expand(con, hop, limit=None):
    before = con.execute("SELECT COALESCE(MAX(id), 0) FROM edge").fetchone()[0]
    sql = """
        INSERT OR IGNORE INTO edge (source, target, tx, time, block_number, token_contract, transfer_index, value)
        SELECT x.source, x.target, x.hash, x.time, x.block_number, x.token_contract, x.transfer_index, x.value
        FROM xfer x JOIN frontier f ON f.address = x.source
        WHERE x.time >= f.cutoff AND (f.address != ? OR x.target = ?)
    """
    params = [COLD, SEED]
    if limit is not None:
        sql += " ORDER BY x.time, x.hash, x.source, x.target, x.transfer_index LIMIT ?"
        params.append(int(limit))
    con.execute(sql, params)
    con.execute("INSERT OR IGNORE INTO expanded SELECT address FROM frontier")
    con.execute(
        """
        INSERT OR IGNORE INTO reached (address, first_time, hop)
        SELECT target, MIN(time), ? FROM edge WHERE id > ? GROUP BY target
        """,
        (hop, before),
    )


def walk(con, hops, start=None, edge_limit=None):
    # start=(address, cutoff) continues the current graph from that wallet.
    if start is None:
        con.execute("DELETE FROM reached")
        con.execute("DELETE FROM expanded")
        con.execute("DELETE FROM edge")
    elif con.execute("SELECT 1 FROM expanded WHERE address = ?", (start[0],)).fetchone():
        return 0, 0, 0
    con.execute("CREATE TEMP TABLE IF NOT EXISTS frontier (address TEXT PRIMARY KEY, cutoff TEXT)")
    nodes_before = con.execute("SELECT COUNT(*) FROM reached").fetchone()[0]
    edges_before = con.execute("SELECT COUNT(*) FROM edge").fetchone()[0]
    if start is None:
        con.execute("INSERT INTO reached VALUES (?, ?, 0)", (COLD, THEFT))
        con.execute("INSERT INTO reached VALUES (?, ?, 0)", (SEED, WINDOW))
    else:
        con.execute("INSERT OR IGNORE INTO reached VALUES (?, ?, 0)", start)
        con.commit()
    done = 0
    for hop in range(1, hops + 1):
        con.execute("DELETE FROM frontier")
        before_edges = con.execute("SELECT COUNT(*) FROM edge").fetchone()[0]
        before_wallets = con.execute("SELECT COUNT(*) FROM reached").fetchone()[0]
        left = None
        if edge_limit is not None:
            left = edge_limit - (before_edges - edges_before)
            if left <= 0:
                break
        if start is None and hop == 1:
            con.execute("INSERT INTO frontier VALUES (?, ?)", (COLD, THEFT))
            expand(con, hop)
            received = con.execute("SELECT MIN(time) FROM edge WHERE target = ?", (SEED,)).fetchone()[0]
            con.execute("UPDATE reached SET first_time = ? WHERE address = ?", (received or WINDOW, SEED))
            con.execute("DELETE FROM frontier")
            if con.execute("SELECT 1 FROM busy WHERE address = ?", (SEED,)).fetchone() is None:
                con.execute("INSERT INTO frontier VALUES (?, ?)", (SEED, received or WINDOW))
                expand(con, hop)
        elif start is not None and hop == 1:
            if con.execute("SELECT 1 FROM busy WHERE address = ?", (start[0],)).fetchone() is None:
                con.execute("INSERT INTO frontier VALUES (?, ?)", start)
                expand(con, hop, left)
        else:
            con.execute(
                """
                INSERT INTO frontier (address, cutoff)
                SELECT r.address, r.first_time
                FROM reached r
                WHERE r.hop = ?
                  AND r.address NOT IN (SELECT address FROM expanded)
                  AND r.address NOT IN (SELECT address FROM busy)
                """,
                (hop - 1,),
            )
            if con.execute("SELECT COUNT(*) FROM frontier").fetchone()[0] == 0:
                break
            expand(con, hop, left)
        new_wallets = con.execute("SELECT COUNT(*) FROM reached").fetchone()[0] - before_wallets
        new_edges = con.execute("SELECT COUNT(*) FROM edge").fetchone()[0] - before_edges
        if start is None:
            log(f"hop {hop}: {new_wallets} new wallets, {new_edges} new transfers")
        con.commit()
        done = hop
        if new_wallets == 0:
            break
    nodes = con.execute("SELECT COUNT(*) FROM reached").fetchone()[0] - nodes_before
    edges = con.execute("SELECT COUNT(*) FROM edge").fetchone()[0] - edges_before
    return done, nodes, edges


def noise(con, hops, ratio):
    """Add forward walks that did not start in the theft graph. Local xfer only."""
    con.execute("PRAGMA synchronous=OFF")
    con.execute("CREATE TABLE IF NOT EXISTS noise_meta (base INTEGER, tx TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS noise_before (address TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE IF NOT EXISTS noise_node (address TEXT PRIMARY KEY, tx TEXT)")
    current = con.execute("SELECT COUNT(*) FROM edge").fetchone()[0]
    row = con.execute("SELECT base, tx FROM noise_meta").fetchone()
    grader = OUT / "grader_noise.csv"
    if row is None:
        con.execute("INSERT INTO noise_meta VALUES (?, NULL)", (current,))
        con.execute("INSERT INTO noise_before SELECT address FROM reached")
        con.commit()
        base = current
    else:
        base = row[0]
        if current - base >= base * ratio:
            log(f"noise already has {current - base} transfers")
            return True
        if row[1]:
            con.execute(
                """
                INSERT OR IGNORE INTO noise_node (address, tx)
                SELECT address, ? FROM reached
                WHERE address NOT IN (SELECT address FROM noise_before)
                  AND address NOT IN (SELECT address FROM noise_node)
                """,
                (row[1],),
            )
            con.commit()
    added = current - base
    goal = base * ratio

    used = 0
    if added < goal:
        log("indexing edges")
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS edge_key ON edge(tx, source, target, token_contract, transfer_index)")
        listed = ",".join("'" + asset + "'" for asset in NOISE_ASSETS)
        con.execute("DROP TABLE IF EXISTS big")
        con.execute("DROP TABLE IF EXISTS want")
        con.execute("DROP TABLE IF EXISTS seen")
        con.execute(
            f"""
            CREATE TEMP TABLE big AS
            SELECT hash, source, target, time
            FROM xfer
            WHERE token_contract IN ({listed})
              AND (length(value) > {len(NOISE_MIN)} OR (length(value) = {len(NOISE_MIN)} AND value >= '{NOISE_MIN}'))
            """
        )
        con.execute("DELETE FROM big WHERE target IN (SELECT address FROM reached)")
        con.execute("DELETE FROM big WHERE source IN (SELECT address FROM expanded)")
        con.execute("CREATE TEMP TABLE want (address TEXT PRIMARY KEY)")
        con.execute("INSERT INTO want SELECT DISTINCT target FROM big")
        con.execute(
            """
            CREATE TEMP TABLE seen AS
            SELECT address, substr(MIN(time), 1, 10) AS day FROM (
                SELECT x.source AS address, x.time AS time FROM xfer x JOIN want w ON w.address = x.source
                UNION ALL
                SELECT x.target, x.time FROM xfer x JOIN want w ON w.address = x.target
            ) GROUP BY address
            """
        )
        fresh, rest = [], []
        for tx, target, when, is_fresh in con.execute(
            """
            SELECT hash, target, time, fresh FROM (
                SELECT b.hash AS hash, b.target AS target, b.time AS time,
                       (s.day = substr(b.time, 1, 10)) AS fresh,
                       ROW_NUMBER() OVER (PARTITION BY b.target ORDER BY b.time, b.hash) AS n
                FROM big b JOIN seen s ON s.address = b.target
            ) WHERE n = 1
            ORDER BY target
            """
        ):
            (fresh if is_fresh else rest).append((tx, target, when))
        rng = random.Random(42)
        rng.shuffle(fresh)
        rng.shuffle(rest)
        seeds = fresh + rest
        log(f"{len(seeds)} noise seeds")
        for tx, target, when in seeds:
            if added >= goal:
                break
            if con.execute("SELECT 1 FROM reached WHERE address = ?", (target,)).fetchone():
                continue
            limit = int(min(goal * NOISE_SEED_SHARE, goal - added))
            if limit < 1:
                break
            con.execute("UPDATE noise_meta SET tx = ?", (tx,))
            con.commit()
            done, nodes, edges = walk(con, hops, start=(target, when), edge_limit=limit)
            con.execute(
                """
                INSERT OR IGNORE INTO expanded
                SELECT address FROM reached
                WHERE address NOT IN (SELECT address FROM noise_before)
                  AND address NOT IN (SELECT address FROM expanded)
                """
            )
            if nodes:
                con.execute(
                    """
                    INSERT OR IGNORE INTO noise_node (address, tx)
                    SELECT address, ? FROM reached
                    WHERE address NOT IN (SELECT address FROM noise_before)
                      AND address NOT IN (SELECT address FROM noise_node)
                    """,
                    (tx,),
                )
            con.commit()
            added += edges
            used += 1
            log(f"noise {tx}: {done} hops, {nodes} new nodes, {edges} new edges")
    node_count = con.execute("SELECT COUNT(*) FROM noise_node").fetchone()[0]
    log(f"noise: {used} seeds, {node_count} new nodes, {added} new edges")
    tmp = grader.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["address", "noise_seed_tx"])
        writer.writerows(con.execute("SELECT address, tx FROM noise_node ORDER BY address"))
    tmp.replace(grader)
    return True


def run_lookup(client, sql, addresses, budget, state, limit=None):
    from google.cloud import bigquery

    if not addresses:
        return []
    params = [bigquery.ArrayQueryParameter("addresses", "STRING", addresses)]
    estimate = int(client.query(
        sql, location="US",
        job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False, query_parameters=params),
    ).total_bytes_processed or 0)
    left = budget - state["spent"]
    log(f"lookup {len(addresses)} addresses, {estimate // 1024**3} GiB")
    if estimate > left or (limit is not None and estimate > limit):
        log("lookup skipped")
        return None
    job = client.query(
        sql, location="US",
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=int(left), query_parameters=params),
    )
    rows = list(job.result())
    state["spent"] += int(job.total_bytes_billed or 0)
    save_state(state)
    return rows


def enrich(client, con, budget, state):
    tokens = [row[0] for row in con.execute("SELECT DISTINCT token_contract FROM edge WHERE token_contract != ''")]
    if tokens and con.execute("SELECT COUNT(*) FROM token").fetchone()[0] == 0:
        rows = run_lookup(
            client,
            f"SELECT LOWER(address) AS address, symbol, name, CAST(decimals AS STRING) AS decimals FROM `{TOKEN_TABLE}` WHERE LOWER(address) IN UNNEST(@addresses)",
            tokens[:50000],
            budget,
            state,
        )
        if rows:
            con.executemany(
                "INSERT OR REPLACE INTO token VALUES (?,?,?,?)",
                [(row["address"], row["symbol"] or "", row["name"] or "", row["decimals"] or "") for row in rows],
            )
    pending = [row[0] for row in con.execute("SELECT address FROM reached WHERE address NOT IN (SELECT address FROM account)")]
    for start in range(0, len(pending), 20000):
        batch = pending[start:start + 20000]
        rows = run_lookup(
            client,
            f"SELECT LOWER(address) AS address, CAST(is_contract AS STRING) AS is_contract FROM `{DATASET}.accounts` WHERE address IN UNNEST(@addresses)",
            batch,
            budget,
            state,
            limit=5 * 1024**3,
        )
        if rows is None:
            break
        con.executemany(
            "INSERT OR REPLACE INTO account VALUES (?,?)",
            [(row["address"], "true" if str(row["is_contract"]).lower() == "true" else "false") for row in rows],
        )
    con.commit()


def earlier(client, con, budget, state):
    # One scan of the 90 days before the theft. Saved as prior.csv so a later run does not scan again.
    if con.execute("SELECT 1 FROM prior_done").fetchone():
        log("earlier activity already saved")
        return
    dest = OUT / "prior.csv"
    if not (dest.exists() and dest.stat().st_size > 0):
        from google.cloud import bigquery

        addrs = OUT / "prior_addrs.csv"
        with addrs.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["address"])
            writer.writerows(con.execute("SELECT address FROM reached"))
        dataset = bigquery.Dataset(f"{client.project}.bybit_prior")
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        table = f"{client.project}.bybit_prior.addrs"
        with addrs.open("rb") as handle:
            client.load_table_from_file(
                handle, table,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    write_disposition="WRITE_TRUNCATE",
                    schema=[bigquery.SchemaField("address", "STRING")],
                ),
            ).result()
        addrs.unlink()
        when = f"block_timestamp >= '{PRIOR_START}' AND block_timestamp < '{START}'"
        sql = f"""
            SELECT t.address, COUNT(*) AS n, MAX(t.ts) AS last_seen
            FROM (
                SELECT addr AS address, ts FROM (
                    SELECT t.block_timestamp AS ts, LOWER(t.from_address) AS a, LOWER(t.to_address) AS b
                    FROM `{DATASET}.transactions` AS t
                    JOIN `{DATASET}.receipts` AS r
                      ON r.transaction_hash = t.transaction_hash AND r.block_timestamp = t.block_timestamp
                    WHERE t.block_timestamp >= '{PRIOR_START}' AND t.block_timestamp < '{START}'
                      AND r.block_timestamp >= '{PRIOR_START}' AND r.block_timestamp < '{START}'
                      AND r.status = 1 AND t.from_address IS NOT NULL AND t.to_address IS NOT NULL
                      AND SAFE_CAST(t.value_lossless AS BIGNUMERIC) > 0
                ) AS s, UNNEST([s.a, IF(s.b = s.a, NULL, s.b)]) AS addr
                JOIN `{table}` AS ad ON ad.address = addr
                UNION ALL
                SELECT addr, ts FROM (
                    SELECT block_timestamp AS ts, LOWER(action.from_address) AS a, LOWER(action.to_address) AS b
                    FROM `{DATASET}.traces`
                    WHERE {when}
                      AND action.from_address IS NOT NULL AND action.to_address IS NOT NULL
                      AND SAFE_CAST(action.value_lossless AS BIGNUMERIC) > 0
                      AND ARRAY_LENGTH(trace_address) > 0
                      AND trace_type IN ('call', 'create')
                      AND (action.call_type IS NULL OR action.call_type NOT IN ('delegatecall', 'staticcall'))
                      AND error IS NULL
                ) AS s, UNNEST([s.a, IF(s.b = s.a, NULL, s.b)]) AS addr
                JOIN `{table}` AS ad ON ad.address = addr
                UNION ALL
                SELECT addr, ts FROM (
                    SELECT block_timestamp AS ts, LOWER(from_address) AS a, LOWER(to_address) AS b
                    FROM `{DATASET}.token_transfers`
                    WHERE {when}
                      AND from_address IS NOT NULL AND to_address IS NOT NULL
                      AND SAFE_CAST(quantity AS BIGNUMERIC) > 0
                      AND token_id IS NULL AND (removed IS NULL OR removed = FALSE)
                ) AS s, UNNEST([s.a, IF(s.b = s.a, NULL, s.b)]) AS addr
                JOIN `{table}` AS ad ON ad.address = addr
            ) AS t
            GROUP BY t.address
        """
        estimate = int(client.query(
            sql, location="US",
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
        ).total_bytes_processed or 0)
        left = budget - state["spent"]
        log(f"earlier activity {estimate // 1024**3} GiB")
        if estimate > left:
            log("over the cap")
            raise SystemExit(2)
        job = client.query(sql, location="US", job_config=bigquery.QueryJobConfig(maximum_bytes_billed=int(left)))
        partial = dest.with_suffix(".csv.partial")
        count = 0
        with partial.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["address", "n", "last_seen"])
            for row in job.result(page_size=20000):
                writer.writerow([row["address"], row["n"], cell(row["last_seen"])])
                count += 1
                if count % 250000 == 0:
                    log(f"earlier activity {count}")
        partial.replace(dest)
        state["spent"] += int(job.total_bytes_billed or 0)
        save_state(state)
        log(f"earlier activity {count} rows")
    batch = []
    con.execute("PRAGMA busy_timeout=21600000")
    con.execute("DELETE FROM prior")
    with dest.open(newline="") as handle:
        for row in csv.DictReader(handle):
            batch.append((row["address"], int(row["n"]), row["last_seen"]))
            if len(batch) >= 5000:
                con.executemany("INSERT INTO prior VALUES (?,?,?)", batch)
                batch = []
    if batch:
        con.executemany("INSERT INTO prior VALUES (?,?,?)", batch)
    con.execute("INSERT INTO prior_done VALUES ('90d')")
    con.commit()
    log(f"earlier activity saved for {con.execute('SELECT COUNT(*) FROM prior').fetchone()[0]} addresses")


def export(con):
    symbols = {row["address"]: row["symbol"] for row in con.execute("SELECT address, symbol FROM token") if row["symbol"]}
    decimals = {row["address"]: row["decimals"] for row in con.execute("SELECT address, decimals FROM token") if row["decimals"] != ""}
    edges = OUT / "edges.csv"
    tmp = edges.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EDGE_FIELDS)
        writer.writeheader()
        for row in con.execute(
            """
            SELECT e.*, t.nonce, t.gas_used, t.gas_price, t.method, t.signer
            FROM edge e LEFT JOIN tx t ON t.hash = e.tx
            ORDER BY e.time, e.tx, e.id
            """
        ):
            token = row["token_contract"] or ""
            places = decimals[token] if token in decimals else (None if token else 18)
            if places is None:
                amount = row["value"]
            else:
                raw = int(Decimal(str(row["value"])))
                whole, frac = divmod(raw, 10 ** int(places))
                amount = str(whole) if frac == 0 else f"{whole}.{frac:0{int(places)}d}".rstrip("0").rstrip(".")
            writer.writerow({
                "source": row["source"],
                "target": row["target"],
                "tx": row["tx"],
                "asset": symbols.get(token, "TOKEN") if token else "ETH",
                "amount": amount,
                "time": row["time"],
                "block_number": row["block_number"],
                "nonce": row["nonce"] or "",
                "gas_used": row["gas_used"] or "",
                "gas_price": row["gas_price"] or "",
                "method": row["method"] or "",
                "token_contract": token,
                "signer": (row["signer"] or "") if token else row["source"],
                "transfer_index": row["transfer_index"] or "",
            })
    tmp.replace(edges)

    nodes = OUT / "nodes.csv"
    tmp = nodes.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=NODE_FIELDS)
        writer.writeheader()
        for row in con.execute(
            """
            WITH counts AS (
                SELECT address, SUM(ins) AS ins, SUM(outs) AS outs FROM (
                    SELECT target AS address, 1 AS ins, 0 AS outs FROM edge
                    UNION ALL
                    SELECT source, 0, 1 FROM edge
                ) GROUP BY address
            )
            SELECT r.address, r.hop, r.first_time,
                   COALESCE(c.ins, 0) AS ins, COALESCE(c.outs, 0) AS outs,
                   COALESCE(a.is_contract, '') AS is_contract,
                   COALESCE(p.n, 0) AS n, COALESCE(p.last_seen, '') AS last_seen
            FROM reached r
            LEFT JOIN counts c ON c.address = r.address
            LEFT JOIN account a ON a.address = r.address
            LEFT JOIN prior p ON p.address = r.address
            WHERE c.address IS NOT NULL OR r.hop = 0
            ORDER BY r.hop, r.address
            """
        ):
            writer.writerow({
                "id": row["address"],
                "is_contract": row["is_contract"],
                "hop": row["hop"],
                "first_received_time": row["first_time"],
                "graph_in_count": row["ins"],
                "graph_out_count": row["outs"],
                "active_before": "true" if row["n"] else "false",
                "tx_count_before": row["n"],
                "last_seen_before": row["last_seen"],
            })
    tmp.replace(nodes)
    with (OUT / "tokens.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["address", "symbol", "name", "decimals"])
        writer.writerows(con.execute("SELECT address, symbol, name, decimals FROM token ORDER BY address"))


def build(hops, rebuild, max_transfers):
    db = OUT / "graph.sqlite"
    if db.exists() and (OUT / "edges.csv").exists() and not rebuild:
        log("graph already built")
        return connect(db)
    if db.exists():
        db.unlink()
    con = connect(db)
    log("loading")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("DELETE FROM tx")
    con.execute("DELETE FROM xfer")
    parents, eth = [], []
    with (OUT / "raw" / "transactions.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            digest = (row.get("hash") or "").lower()
            signer = (row.get("signer") or "").lower()
            if not digest or not signer:
                continue
            parents.append((digest, signer, row.get("nonce") or "", row.get("gas_used") or "", row.get("gas_price") or "", (row.get("method") or "").lower()))
            raw = row.get("value")
            amount = "" if raw in (None, "", "0", "0.0") else str(int(Decimal(raw)))
            target = (row.get("to_address") or "").lower()
            if target and amount:
                eth.append((signer, target, digest, amount, norm_time(row["block_timestamp"]), row.get("block_number") or "", "", ""))
            if len(parents) >= 5000:
                con.executemany("INSERT OR REPLACE INTO tx VALUES (?,?,?,?,?,?)", parents)
                con.executemany("INSERT INTO xfer VALUES (?,?,?,?,?,?,?,?)", eth)
                parents, eth = [], []
    con.executemany("INSERT OR REPLACE INTO tx VALUES (?,?,?,?,?,?)", parents)
    con.executemany("INSERT INTO xfer VALUES (?,?,?,?,?,?,?,?)", eth)
    con.commit()
    log("loaded transactions")

    con.execute("CREATE TEMP TABLE stage (source TEXT, target TEXT, hash TEXT, value TEXT, time TEXT, block_number TEXT, token_contract TEXT, transfer_index TEXT)")
    for name in ("traces", "token_transfers"):
        batch = []
        with (OUT / "raw" / f"{name}.csv").open(newline="") as handle:
            for row in csv.DictReader(handle):
                source = (row.get("source") or "").lower()
                target = (row.get("target") or "").lower()
                digest = (row.get("hash") or "").lower()
                raw = row.get("value")
                amount = "" if raw in (None, "", "0", "0.0") else str(int(Decimal(raw)))
                if not source or not target or not digest or not amount:
                    continue
                batch.append((source, target, digest, amount, norm_time(row["block_timestamp"]), row.get("block_number") or "", (row.get("token_contract") or "").lower(), row.get("transfer_index") or ""))
                if len(batch) >= 5000:
                    con.executemany("INSERT INTO stage VALUES (?,?,?,?,?,?,?,?)", batch)
                    con.execute(
                        """
                        INSERT INTO xfer
                        SELECT source, target, hash, value, time, block_number, token_contract, transfer_index
                        FROM stage WHERE hash IN (SELECT hash FROM tx)
                        """
                    )
                    con.execute("DELETE FROM stage")
                    batch = []
        if batch:
            con.executemany("INSERT INTO stage VALUES (?,?,?,?,?,?,?,?)", batch)
            con.execute(
                """
                INSERT INTO xfer
                SELECT source, target, hash, value, time, block_number, token_contract, transfer_index
                FROM stage WHERE hash IN (SELECT hash FROM tx)
                """
            )
            con.execute("DELETE FROM stage")
        con.commit()
        log(f"loaded {name}")
    con.execute("CREATE INDEX IF NOT EXISTS xfer_source ON xfer(source, time)")
    con.commit()
    con.execute("DELETE FROM busy")
    con.execute(
        """
        INSERT INTO busy (address, n)
        SELECT source, COUNT(*) FROM xfer
        GROUP BY source
        HAVING COUNT(*) > ?
        """,
        (max_transfers,),
    )
    con.commit()
    log("walking")
    walk(con, hops)
    return con


def main():
    global LOG
    parser = argparse.ArgumentParser(description="Download the theft graph from BigQuery into out/")
    parser.add_argument("--project", default="")
    parser.add_argument("--noise", action="store_true")
    parser.add_argument("--rebuild-graph", action="store_true")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    LOG = OUT / "download.log"
    if args.noise:
        db = OUT / "graph.sqlite"
        if not db.exists():
            raise SystemExit(f"missing {db}")
        log("adding noise")
        con = connect(db)
        if noise(con, HOPS, 1.0):
            export(con)
            log("saved")
        return
    if not args.project:
        raise SystemExit("python3 download_graph.py --project YOUR_PROJECT_ID")
    try:
        from google.cloud import bigquery
        from google.auth.exceptions import DefaultCredentialsError
    except ImportError:
        raise SystemExit("pip install google-cloud-bigquery")
    try:
        client = bigquery.Client(project=args.project, location="US")
    except DefaultCredentialsError:
        raise SystemExit("gcloud auth application-default login")

    state = load_state()
    log(f"writing to {OUT}")
    for name, sql in queries().items():
        download_table(client, name, sql, BUDGET, state)
    con = build(HOPS, args.rebuild_graph, 50000)
    enrich(client, con, BUDGET, state)
    earlier(client, con, BUDGET, state)
    export(con)
    log("done")


if __name__ == "__main__":
    main()
