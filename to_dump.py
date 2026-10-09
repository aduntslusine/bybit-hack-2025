"""Save nodes.csv and edges.csv as out/graph.dump for Neo4j Desktop."""

import csv
import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
CACHE = Path.home() / "Library/Application Support/neo4j-desktop/Application/Cache"
DROP = {"hop", "first_received_time", "graph_in_count", "graph_out_count"}
HEADERS = {
    "nodes.csv": "address:ID(Address),is_contract,active_before:boolean,tx_count_before:int,last_seen_before",
    "edges.csv": ":START_ID(Address),:END_ID(Address),tx,asset,amount,time,block_number,nonce,gas_used,gas_price,method,token_contract,signer,transfer_index",
}


def copy_csv(name):
    with (OUT / name).open(newline="") as src, (OUT / "dump-work" / name).open("w", newline="") as dest:
        reader = csv.DictReader(src)
        fields = [field for field in reader.fieldnames if field not in DROP]
        dest.write(HEADERS[name] + "\n")
        writer = csv.DictWriter(dest, fieldnames=fields, lineterminator="\n", extrasaction="ignore")
        for row in reader:
            writer.writerow(row)


def main():
    javas = sorted(CACHE.glob("runtime/*/Contents/Home"))
    neos = sorted(CACHE.glob("dbmss/neo4j-enterprise-*"))
    if not javas or not neos:
        raise SystemExit("Neo4j Desktop is not installed")
    work = OUT / "dump-work"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir()
    copy_csv("nodes.csv")
    copy_csv("edges.csv")
    data = (work / "data").resolve()
    conf = work / "neo4j.conf"
    conf.write_text(
        f"server.directories.data={data}\n"
        f"server.directories.transaction.logs.root={data / 'transactions'}\n"
        "server.memory.heap.initial_size=2g\n"
        "server.memory.heap.max_size=4g\n"
    )
    env = {**os.environ, "JAVA_HOME": str(javas[-1])}
    admin = str(neos[-1] / "bin" / "neo4j-admin")
    subprocess.run([
        admin, "database", "import", "full",
        "--overwrite-destination=true",
        "--id-type=string",
        "--max-off-heap-memory=4G",
        f"--additional-config={conf}",
        f"--nodes=Address={work / 'nodes.csv'}",
        f"--relationships=TRANSFER={work / 'edges.csv'}",
        "--", "graph",
    ], check=True, env=env)
    subprocess.run([
        admin, "database", "dump", "graph",
        "--overwrite-destination=true",
        f"--to-path={OUT}",
        f"--additional-config={conf}",
    ], check=True, env=env)
    shutil.rmtree(work)
    print(OUT / "graph.dump")


if __name__ == "__main__":
    main()
