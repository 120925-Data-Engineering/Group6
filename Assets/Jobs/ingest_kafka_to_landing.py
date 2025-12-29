"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""
from kafka import KafkaConsumer
import json
import time
import os
import argparse


def consume_batch(topic: str, batch_duration_sec: int, output_path: str) -> int:
    """
    Consume from Kafka for specified duration and write to landing zone.
    
    Args:
        topic: Kafka topic to consume from
        batch_duration_sec: How long to consume before writing
        output_path: Directory to write output JSON files
        
    Returns:
        Number of messages consumed
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers = ["kafka:9092"],
        auto_offset_reset = "latest",
        enable_auto_commit = True,
        value_deserializer = lambda v: json.loads(v.decode('utf-8'))
    )
    
    records = consumer.poll(timeout_ms = batch_duration_sec*1000)
    
    for record in records:
        with open(f"./data/landing/{topic}.json", "a") as f:
            f.write(json.dumps(record.value) + '\n')


if __name__ == "__main__":
    # TODO: Parse args and call consume_batch
    transaction_topic = "transaction_events"
    user_topic = "user_events"
    pass
