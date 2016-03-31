# -*- coding: utf-8 -*-

import sys
import json
import yaml
import argparse
import bruce
from time import time

from config import validate_config
from utils import usage, java_string_hashcode
from row_generator import RowGenerator
from kafka import KafkaProducer

UPDATE_PERIOD_MILLIS = 250
DISPLAY_PROGRESS_PERIOD_MILLIS = 250
DISPLAY_PROGRESS_WARMUP_MILLIS = 5000

def display_line(line):
    ansiClearLine = "\033[K"
    ansiMoveCursorToColumnZero = "\033[0G"
    sys.stderr.write(ansiClearLine + ansiMoveCursorToColumnZero + line)
    sys.stderr.flush()

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

def produce(f_produce, topic, partition, key, value):
    f_produce(topic, key = key, value = value, partition = partition)

def maxwell_message(database, table, type, data, pk_name, pk_value):
    key = json.dumps({
        "database": database,
        "table": table,
        "pk." + pk_name: pk_value
    }, separators=(',',':'))
    value = json.dumps({
        "database": database,
        "table": table,
        "type": type,
        "ts": int(time()),
        "data": data
    }, separators=(',',':'))
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

def bootstrap(f_produce, partition_count, schema, database, table, config):
    topic = config['kafka']['topic']
    start_time_millis = time() * 1000.0
    inserted_rows = 0
    total_rows = int(float(config['mysql']['schemas'][schema]['tables'][table][database]['size']))

    partition = abs(java_string_hashcode(database) % partition_count)
    produce(f_produce, topic, partition, *bootstrap_start_message(schema, database, table, config))
    last_display_progress = time()
    for key, value in bootstrap_insert_messages(schema, database, table, config, total_rows):
        produce(f_produce, topic, partition, key, value)
        inserted_rows += 1
        if time() - last_display_progress > (DISPLAY_PROGRESS_PERIOD_MILLIS / 1000.0):
            display_progress(total_rows, inserted_rows, start_time_millis)
            last_display_progress = time()
    produce(f_produce, topic, partition, *bootstrap_complete_message(schema, database, table, config))
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
    parser.add_argument('--target', metavar='TARGET', type=str, required=True,
                        help='target system that messages will be output to')
    parser.add_argument('--partition-count', metavar='PARTITION_COUNT', type=int, required=False,
                        help='number of partitions (will read from kafka topic if not specified)')
    args = parser.parse_args()
    config = yaml.load(open(args.config).read())
    validate_config(config)
    schema = find_schema(config, args.database, args.table)

    if args.target == 'console':
        produce_to_console(schema, args, config)
    elif args.target == 'kafka':
        produce_to_kafka(schema, args, config)
    elif args.target == 'bruce':
        produce_to_bruce(schema, args, config)
    else:
        raise Exception('invalid target')


def produce_to_kafka(schema, args, config):
    topic = config['kafka']['topic']
    producer = KafkaProducer(bootstrap_servers = config['kafka']['brokers'])

    def f_produce(topic, partition, key, value):
        producer.send(topic, key = key, value = value, partition = partition)

    partition_count = 1 + max(producer.partitions_for(topic))
    try:
        bootstrap(f_produce, partition_count, schema, args.database, args.table, config)
    except KeyboardInterrupt:
        sys.exit(1)
    producer.flush()
    producer.close()


def produce_to_bruce(schema, args, config):
    topic = config['kafka']['topic']

    if args.partition_count:
        partition_count = args.partition_count
    else:
        print 'fetch partition info for topic ' + topic
        producer = KafkaProducer(bootstrap_servers = config['kafka']['brokers'])
        partition_count = 1 + max(producer.partitions_for(topic))
        producer.close()

    socket = bruce.open_bruce_socket()

    # batching socket send
    buff = []

    def flush_buff():
        for msg in buff:
            socket.sendto(msg, '/var/run/bruce/bruce.socket')
        del buff[:]

    def f_produce(topic, partition, key, value):
        if len(buff) < 1000:
            buff.append(bruce.create_msg(partition, topic, bytes(key), bytes(value)))
        else:
            flush_buff()

    try:
        bootstrap(f_produce, partition_count, schema, args.database, args.table, config)
        flush_buff()
    except KeyboardInterrupt:
        sys.exit(1)
    finally:
        socket.close()


def produce_to_console(schema, args, config):
    # 12 by default
    partition_count = 12
    if args.partition_count:
        partition_count = args.partition_count

    def f_produce(topic, partition, key, value):
        print json.dumps({
            "partition": partition,
            "key": key,
            "message": value
        }, separators=(',',':'))

    try:
        bootstrap(f_produce, partition_count, schema, args.database, args.table, config)
    except KeyboardInterrupt:
        sys.exit(1)

