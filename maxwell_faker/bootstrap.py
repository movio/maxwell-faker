# -*- coding: utf-8 -*-

import sys
import json
import yaml
import argparse
from time import time

from config import validate_config
from utils import usage, java_string_hashcode
from row_generator import RowGenerator
from kafka import KafkaProducer

UPDATE_PERIOD_MILLIS = 250
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

def produce(producer, topic, partition, message):
    producer.send(topic, key = "hello", value = message, partition = partition)

def maxwell_message(database, table, type, data):
    return json.dumps({
        "database": database,
        "table": table,
        "type": type,
        "ts": int(time()),
        "data": data
    })

def bootstrap_start_message(database, table):
    return maxwell_message(database, table, "bootstrap-start", {})

def bootstrap_complete_message(database, table):
    return maxwell_message(database, table, "bootstrap-complete", {})

def bootstrap(producer, schema, database, table, config):
    topic = config['kafka']['topic']
    start_time_millis = time() * 1000.0
    inserted_rows = 0
    total_rows = int(float(config['mysql']['schemas'][schema]['tables'][table][database]['bootstrap-count']))
    partition_count = 1 + max(producer.partitions_for(topic))
    partition = abs(java_string_hashcode(database) % partition_count)
    produce(producer, topic, partition, bootstrap_start_message(database, table))
    for message in bootstrap_insert_messages(schema, database, table, config, total_rows):
        produce(producer, topic, partition, message)
        inserted_rows += 1
        display_progress(total_rows, inserted_rows, start_time_millis)
    produce(producer, topic, partition, bootstrap_complete_message(database, table))
    display_line("")

def bootstrap_insert_messages(schema, database, table, config, rows_total):
    row_generator = RowGenerator.get_instance(schema, database, table, config)
    for row_index in xrange(rows_total):
        yield maxwell_message(database, table, "bootstrap-insert", row_generator.generate_row(row_index))

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
    producer = KafkaProducer(bootstrap_servers = config['kafka']['brokers'])
    try:
        bootstrap(producer, schema, args.database, args.table, config)
    except IOError, e:
        usage(e)
    except KeyboardInterrupt:
        print
    finally:
        print "flushing producer..." + 20 * " "
        producer.flush()
        producer.close()
        print "done"
