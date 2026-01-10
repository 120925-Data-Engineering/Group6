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
    # Creates a kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers = ["kafka:9092"],
        auto_offset_reset = "earliest",
        enable_auto_commit = True,
        group_id = "landing_group",
        value_deserializer = lambda v: json.loads(v.decode('utf-8'))
    )
    
    # Creates the end time when we need to stop listening
    end_time = time.time() + batch_duration_sec   
    
    # Collects the current time
    timestamp = time.time()
    
    # How many messages were consumed
    consumed = 0
    
    # Writes a file to the Bronze zone
    with open(f"{output_path}/{topic}_{timestamp}.json", "a") as f:
        # While time is left don't end the loop
        while time.time() < end_time:
            # Checks if there are any new messages from the topic
            records = consumer.poll(timeout_ms = 10000) # This waits 10 seconds before returning
            # returns nothing if time is passed or returns up to 500 messages back
            
            # Loops through the batch of messages returned
            for batch_records in records.values():
                
                # Collects how much are consumed in this batch
                consumed += len(batch_records)
                
                # Collects each specific message for each batch
                for message in batch_records:
                    
                    # Adds the message value to a list of messages to be saved
                    f.write(json.dumps(message.value) + '\n')
                    
    # Closes the consumer
    consumer.close()
    
    return f"Read {topic} for {batch_duration_sec*1000} ms and written to {output_path}/{topic}_{timestamp}.json and consumed {consumed} ammount of records"


if __name__ == "__main__":
    # TODO: Parse args and call consume_batch
    parser = argparse.ArgumentParser(description = "Subscribing to a Kafka server and writing to a json file")
    parser.add_argument("--topic", default= "user_events", help = "Kafka topic name")
    parser.add_argument("--batch-time", default = "30", help = "How long it subscribes for in seconds")
    parser.add_argument("--output-path", default = "./data/landing", help = "Where is the folder where the files are saved")
    
    args = parser.parse_args()
    
    output_path = args.output_path
    topic = args.topic
    batch_time = int(args.batch_time)
    
    topic_Info = consume_batch(topic = topic,batch_duration_sec = batch_time,output_path = output_path)
    print(topic_Info)
