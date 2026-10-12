"""Emit raw OMS orders (JSON lines) for the oms.orders.raw topic.

  python3.11 orders_generator.py --n 50 [--schema-version 1|2|3] | scripts/kafka.sh produce oms.orders.raw

Schema versions simulate an upstream OMS release:
  v1  baseline
  v2  + venue, + order_type                    (additive, non-breaking)
  v3  acct renamed to account_ref, px sent as a string   (breaking: removal + type change)
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timezone

FUNDS = {"EQIDX": 513.0, "TOTMK": 129.0, "BNDAG": 9.8, "INTLX": 33.5}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--schema-version", type=int, default=1, choices=[1, 2, 3])
    a = ap.parse_args()
    for _ in range(a.n):
        t = random.choice(list(FUNDS))
        o = {
            "ord_id": f"O-{uuid.uuid4().hex[:10]}",
            "acct": f"A{random.randint(100, 120)}",
            "ticker": t,
            "side": random.choice(["buy", "sell", "BUY"]),
            "qty": random.randint(1, 500),
            "px": round(FUNDS[t] * random.uniform(0.99, 1.01), 2),
            "ts": datetime.now(timezone.utc).isoformat(),
            "status": random.choice(["FILLED", "FILLED", "FILLED", "CANCELLED", "PENDING"]),
        }
        if a.schema_version >= 2:
            o["venue"] = random.choice(["NYSE", "NASDAQ", "ARCA", "DARK"])
            o["order_type"] = random.choice(["MARKET", "LIMIT"])
        if a.schema_version >= 3:
            o["account_ref"] = o.pop("acct")
            o["px"] = f"{o['px']:.2f}"
        print(json.dumps(o))
