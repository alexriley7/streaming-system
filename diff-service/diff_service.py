import json
import os
import time
from datetime import datetime
from threading import Thread, Lock

from kafka import KafkaConsumer


# ============================================================
# CONFIG
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
)

GROUP_ID = os.getenv(
    "GROUP_ID",
    "shadow-diff-service"
)

V1_TOPIC = os.getenv(
    "V1_TOPIC",
    "output-topic-v1"
)

SHADOW_TOPIC = os.getenv(
    "SHADOW_TOPIC",
    "shadow-output-topic"
)

OUTPUT_FILE = os.getenv(
    "OUTPUT_FILE",
    "/data/diff-results.json"
)

FLUSH_INTERVAL_SECONDS = 180

# wait for matching record before declaring missing
MATCH_TIMEOUT_SECONDS = 120

# remove old matched records
CLEANUP_INTERVAL_SECONDS = 60


# ============================================================
# GLOBAL STATE
# ============================================================

lock = Lock()

# transactionId -> { payload, received_at }
v1_records = {}

# transactionId -> { payload, received_at }
shadow_records = {}

stats = {
    "v1_count": 0,
    "shadow_count": 0,
    "matched": 0,
    "payload_mismatches": 0,
    "missing_in_shadow": 0,
    "missing_in_v1": 0,
    "duplicate_keys_v1": 0,
    "duplicate_keys_shadow": 0,
    "total_compared": 0
}

mismatch_examples = []


# ============================================================
# HELPERS
# ============================================================

def get_transaction_key(message):

    return message.get("transactionId")


def compare_payloads(v1_payload, shadow_payload):

    mismatches = []

    required_fields = [
        "userId",
        "transactionId",
        "amount",
        "currency",
        "highValue",
        "normalizedCurrency"
    ]

    for field in required_fields:

        if v1_payload.get(field) != shadow_payload.get(field):

            mismatches.append({
                "field": field,
                "v1": v1_payload.get(field),
                "shadow": shadow_payload.get(field)
            })

    # --------------------------------------------------------
    # TIMESTAMP TOLERANCE
    # --------------------------------------------------------

    v1_ts = v1_payload.get("timestamp")
    shadow_ts = shadow_payload.get("timestamp")

    if v1_ts and shadow_ts:

        diff_ms = abs(v1_ts - shadow_ts)

        # allow 5 second difference
        if diff_ms > 5000:

            mismatches.append({
                "field": "timestamp",
                "v1": v1_ts,
                "shadow": shadow_ts,
                "difference_ms": diff_ms
            })

    return mismatches


def create_consumer(topic):

    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{GROUP_ID}-{topic}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )


# ============================================================
# MATCHING LOGIC
# ============================================================

def process_v1_message(message):

    global stats

    tx_key = get_transaction_key(message)

    if not tx_key:
        return

    now = time.time()

    with lock:

        stats["v1_count"] += 1

        if tx_key in v1_records:
            stats["duplicate_keys_v1"] += 1

        v1_records[tx_key] = {
            "payload": message,
            "received_at": now
        }

        if tx_key in shadow_records:

            shadow_msg = shadow_records[tx_key]["payload"]

            stats["total_compared"] += 1

            mismatches = compare_payloads(message, shadow_msg)

            if len(mismatches) == 0:

                stats["matched"] += 1

            else:

                stats["payload_mismatches"] += 1

                mismatch_examples.append({
                    "transactionId": tx_key,
                    "mismatches": mismatches,
                    "v1": message,
                    "shadow": shadow_msg
                })


def process_shadow_message(message):

    global stats

    tx_key = get_transaction_key(message)

    if not tx_key:
        return

    now = time.time()

    with lock:

        stats["shadow_count"] += 1

        if tx_key in shadow_records:
            stats["duplicate_keys_shadow"] += 1

        shadow_records[tx_key] = {
            "payload": message,
            "received_at": now
        }

        if tx_key in v1_records:

            v1_msg = v1_records[tx_key]["payload"]

            stats["total_compared"] += 1

            mismatches = compare_payloads(v1_msg, message)

            if len(mismatches) == 0:

                stats["matched"] += 1

            else:

                stats["payload_mismatches"] += 1

                mismatch_examples.append({
                    "transactionId": tx_key,
                    "mismatches": mismatches,
                    "v1": v1_msg,
                    "shadow": message
                })


# ============================================================
# CONSUMER THREADS
# ============================================================

def consume_v1():

    consumer = create_consumer(V1_TOPIC)

    print(f"Listening to topic: {V1_TOPIC}")

    for msg in consumer:

        try:
            process_v1_message(msg.value)

        except Exception as e:
            print(f"Error processing v1 message: {e}")


def consume_shadow():

    consumer = create_consumer(SHADOW_TOPIC)

    print(f"Listening to topic: {SHADOW_TOPIC}")

    for msg in consumer:

        try:
            process_shadow_message(msg.value)

        except Exception as e:
            print(f"Error processing shadow message: {e}")


# ============================================================
# CLEANUP OLD RECORDS
# ============================================================

def cleanup_old_records():

    while True:

        time.sleep(CLEANUP_INTERVAL_SECONDS)

        now = time.time()

        with lock:

            # ------------------------------------------------
            # REMOVE OLD MATCHED RECORDS
            # ------------------------------------------------

            v1_to_delete = []
            shadow_to_delete = []

            for tx_key, value in v1_records.items():

                age = now - value["received_at"]

                if age > MATCH_TIMEOUT_SECONDS:

                    if tx_key not in shadow_records:
                        stats["missing_in_shadow"] += 1

                    v1_to_delete.append(tx_key)

            for tx_key, value in shadow_records.items():

                age = now - value["received_at"]

                if age > MATCH_TIMEOUT_SECONDS:

                    if tx_key not in v1_records:
                        stats["missing_in_v1"] += 1

                    shadow_to_delete.append(tx_key)

            for tx_key in v1_to_delete:
                del v1_records[tx_key]

            for tx_key in shadow_to_delete:
                del shadow_records[tx_key]

            print(
                f"Cleanup complete. "
                f"v1 cache={len(v1_records)}, "
                f"shadow cache={len(shadow_records)}"
            )


# ============================================================
# PERIODIC REPORT WRITER
# ============================================================

def flush_results():

    while True:

        time.sleep(FLUSH_INTERVAL_SECONDS)

        with lock:

            match_rate = 0

            if stats["total_compared"] > 0:

                match_rate = round(
                    stats["matched"] / stats["total_compared"] * 100,
                    2
                )

            report = {
                "timestamp": datetime.utcnow().isoformat(),
                "stats": stats,
                "match_rate_percent": match_rate,
                "cached_v1_records": len(v1_records),
                "cached_shadow_records": len(shadow_records)
                #"recent_mismatches": mismatch_examples[-20:]
            }

            os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

            with open(OUTPUT_FILE, "w") as f:
                json.dump(report, f, indent=2)

            print("================================================")
            print("DIFF REPORT WRITTEN")
            print(json.dumps(report, indent=2))
            print("================================================")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    print("================================================")
    print("Starting Kafka Shadow Diff Service")
    print("================================================")

    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"V1 topic: {V1_TOPIC}")
    print(f"Shadow topic: {SHADOW_TOPIC}")
    print(f"Output file: {OUTPUT_FILE}")

    Thread(target=consume_v1, daemon=True).start()
    Thread(target=consume_shadow, daemon=True).start()
    Thread(target=flush_results, daemon=True).start()
    Thread(target=cleanup_old_records, daemon=True).start()

    while True:
        time.sleep(60)