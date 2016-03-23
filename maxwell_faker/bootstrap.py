# -*- coding: utf-8 -*-

import sys
import json
import yaml
import argparse
from time import time

from config import validate_config
from utils import usage, java_string_hashcode
from row_generator import RowGenerator
from pykafka import KafkaClient

UPDATE_PERIOD_MILLIS = 250
DISPLAY_PROGRESS_PERIOD_MILLIS = 250
DISPLAY_PROGRESS_WARMUP_MILLIS = 5000

def display_line(line):
    ansiClearLine = "\033[K"
    ansiMoveCursorToColumnZero = "\033[0G"
    sys.stdout.write(ansiClearLine + ansiMoveCursorToColumnZero + line)
    sys.stdout.flush()

def display_progress(total, count, started_time_millis):
    current_time_millis = time() * 1000.0
    elapsed_millis = current_time_millis - started_time_millis
    predicted_total_millis = (elapsed_millis / count) * total
    remaining_millis = predicted_total_millis - elapsed_millis
    duration = pretty_duration(remaining_millis, elapsed_millis)
    display_line("%d / %d (%.2f%%) %s" % (count, total, ( count * 100.0 ) / total, duration))

def pretty_duration(millis, elapsedMillis):
    if elapsedMillis < DISPLAY_PROGRESS_WARMUP_MILLIS:
        return ""
    d = (millis / (1000 * 60 * 60 * 24))
    h = (millis / (1000 * 60 * 60)) % 24
    m = (millis / (1000 * 60)) % 60
    s = (millis / (1000)) % 60
    if d > 0:
        return "- %d days %02dh %02dm %02ds remaining " % (d, h, m, s)
    elif h > 0:
        return "- %02dh %02dm %02ds remaining " % (h, m, s)
    elif m > 0:
        return "- %02dm %02ds remaining " % (m, s)
    elif s > 0:
        return "- %02ds remaining " % s
    else:
        return ""

def produce(producer, topic, partition, key, value):
    # producer.send(topic, key = key, value = value, partition = partition)
    producer.produce(value, partition_key=key)

def maxwell_message(database, table, type, data, pk_name, pk_value):
    key = json.dumps({
        "database": database,
        "table": table,
        "pk." + pk_name: pk_value
    })
    value = json.dumps({
        "database": database,
        "table": table,
        "type": type,
        "ts": int(time()),
        "data": data
    })
    return key, value

def bootstrap_start_message(schema, database, table, config):
    row_generator = RowGenerator.get_instance(schema, database, table, config)
    data = {}
    pk_name = row_generator.primary_key_field
    pk_value = None
    return maxwell_message(database, table, "bootstrap-start", data, pk_name, pk_value)

def bootstrap_complete_message(schema, database, table, config):
    row_generator = RowGenerator.get_instance(schema, database, table, config)
    data = {}
    pk_name = row_generator.primary_key_field
    pk_value = None
    return maxwell_message(database, table, "bootstrap-complete", data, pk_name, pk_value)

def bootstrap(producer, schema, database, table, config):
    topic = None # FIXME
    start_time_millis = time() * 1000.0
    inserted_rows = 0
    total_rows = int(float(config['mysql']['schemas'][schema]['tables'][table][database]['size']))
    partition_count = 1 # FIXME
    partition = abs(java_string_hashcode(database) % partition_count)
    produce(producer, topic, partition, *bootstrap_start_message(schema, database, table, config))
    last_display_progress = time()
    for key, value in bootstrap_insert_messages(schema, database, table, config, total_rows):
        produce(producer, topic, partition, key, value)
        inserted_rows += 1
        if time() - last_display_progress > (DISPLAY_PROGRESS_PERIOD_MILLIS / 1000.0):
            display_progress(total_rows, inserted_rows, start_time_millis)
            last_display_progress = time()
    produce(producer, topic, partition, *bootstrap_complete_message(schema, database, table, config))
    display_line("")

def bootstrap_insert_messages(schema, database, table, config, rows_total):
    row_generator = RowGenerator.get_instance(schema, database, table, config)
    for row_index in xrange(rows_total):
        data = row_generator.generate_row(row_index)
        pk_name = row_generator.primary_key_field
        pk_value = row_generator.generate_primary_key(row_index)
        yield maxwell_message(database, table, "bootstrap-insert", data, pk_name, pk_value)

def find_schema(config, database, table):
    found_schema = None
    for schema in config['mysql']['schemas']:
        databases = config['mysql']['schemas'][schema]['databases']
        tables = config['mysql']['schemas'][schema]['tables']
        if database in databases and table in tables:
            if found_schema: usage('two different schemas cannot have identical database names')
            found_schema = schema
    if not found_schema: usage('could not find schema with specified database and table')
    return found_schema

def main():
    parser = argparse.ArgumentParser(description='Maxwell faker for systems and load testing.')
    parser.add_argument('--config', metavar='CONFIG', type=str, required=True, help='path to yaml config file')
    parser.add_argument('--database', metavar='DATABASE', type=str, required=True, help='database to bootstrap')
    parser.add_argument('--table', metavar='TABLE', type=str, required=True, help='table to bootstrap')
    args = parser.parse_args()
    config = yaml.load(open(args.config).read())
    validate_config(config)
    schema = find_schema(config, args.database, args.table)
    # producer = KafkaProducer(bootstrap_servers = config['kafka']['brokers'], buffer_memory = 335544320)
    client = KafkaClient(hosts=",".join(config['kafka']['brokers']))
    try:
        topic = client.topics[config['kafka']['topic']]
        with topic.get_producer(linger_ms=300, min_queued_messages=20000, max_queued_messages=20000) as producer:
            print "bootstrapping..."
            bootstrap(producer, schema, args.database, args.table, config)
            print "bootstrapping done"
            producer.stop()
            print "producer stopped"
    except KeyboardInterrupt:
        sys.exit(1)
