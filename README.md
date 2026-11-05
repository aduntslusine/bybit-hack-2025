# Bybit theft graph

Public Ethereum transfers around the Bybit cold-wallet theft on 21 February 2025. The cold wallet `0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4` was drained to `0x47666fab8bd0ac7003bce3f5c3585383f09486e2`. This repo downloads that window from BigQuery and follows the funds forward.

The download is public chain data from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us`.

## How the graph is constructed

**1. Download:** Direct ETH transfers, internal ETH transfers from contract calls, and ERC-20 token transfers are pulled from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us` for the window **21 Feb – 7 Mar 2025** and written to `out/raw/`.

**2. Local graph walk:** The raw transfers are loaded into a local SQLite database (`out/graph.sqlite`). A breadth-first walk then traces funds forward from two starting points:

- **Cold wallet** `0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4` — starting at the moment of the theft, `2025-02-21 14:13:35 UTC`
- **First receiver** `0x47666fab8bd0ac7003bce3f5c3585383f09486e2` — starting at the time it first received funds from the cold wallet

At each hop, a wallet is expanded only once, and only transfers at or after the time it first received traced funds are followed. The walk goes up to 10 hops. A wallet with more than 50,000 outgoing transfers is treated as a high-volume router and is not expanded.

**3. Enrichment:** Token metadata (symbol, name, decimals) to decode ERC-20 amounts, and a scan of the 90 days before the theft to set an `active_before` flag per address.

**4. Noise (optional):** `--noise` samples a small set of unrelated ETH transfers from the same window and adds them to the graph. The addresses introduced are written to `out/noise_nodes.csv`.

**5. Export:** The final SQLite graph is exported to three CSVs in `out/`:

| File | Contents |
|---|---|
| `nodes.csv` | One row per address: hop distance, contract flag, prior-activity stats |
| `edges.csv` | One row per transfer: amount (decimal-scaled), asset symbol, timestamp, gas info |
| `tokens.csv` | Symbol, name, decimals for every ERC-20 in the graph |

## Neo4j

Addresses are nodes. A transfer is a relationship between two addresses.

<table><tr>
<td><img src="assets/schema.png" width="340"/></td>
<td><img src="assets/graph.png" width="560"/></td>
</tr></table>

A pre-built dump is hosted at `s3://gds-public-dataset/bybit-hack-graph.dump`. Load it with:

```bash
neo4j-admin database load graph --from-path=.
```

## Run

```bash
pip install google-cloud-bigquery
gcloud auth application-default login
python3 download_graph.py --project YOUR_PROJECT_ID
python3 download_graph.py --project YOUR_PROJECT_ID --noise
```

`--project` is the Google Cloud project that runs the query. The script stops before a query would pass a 900 GiB cap. A later run skips tables already saved under `out/raw/` and keeps the graph when `out/graph.sqlite` and `out/edges.csv` already exist. `--rebuild-graph` loads the raw files again and walks from the cold wallet. `out/` is generated and not committed.