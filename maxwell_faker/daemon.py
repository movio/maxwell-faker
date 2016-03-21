# -*- coding: utf-8 -*-
import sys
import json
import yaml
import argparse
from time import time
from time import sleep

from config import validate_config
from utils import usage, pseudorandom_long, java_string_hashcode
from row_generator import RowGenerator
from kafka import KafkaProducer


def maxwell_message(database, table, operation, data, pk_name, pk_value):
    key = {
        "database": database,
        "table": table,
        "pk." + pk_name: pk_value
    }

    value = {
        "database": database,
        "table": table,
        "type": operation,
        "ts": int(time()),
        "data": data
    }
    return key, value


def main():
    parser = argparse.ArgumentParser(description='Fake Maxwell data into Kafka.')
    parser.add_argument('--config', metavar='CONFIG', type=str, required=True, help='path to yaml config file')
    parser.add_argument('--schema', metavar='SCHEMA', type=str, required=False, help='schema to produce')
    parser.add_argument('--database', metavar='DATABASE', type=str, required=False, help='database to produce')
    parser.add_argument('--table', metavar='TABLE', type=str, required=False, help='table to produce')
    parser.add_argument('-c', action='store_true', required=False, help='produce message to console')
    args = parser.parse_args()

    config = yaml.load(open(args.config).read())
    validate_config(config)

    try:
        f_consume = generate_console_consumer() if args.c else generate_kafka_producer_consumer(config)
        produce_messages(f_consume, args, config)
    except IOError, e:
        usage(e)
    except KeyboardInterrupt:
        sys.exit(1)


def generate_kafka_producer_consumer(config):
    topic = config['kafka']['topic']
    kafka_producer = KafkaProducer(bootstrap_servers=config['kafka']['brokers'])
    partition_count = 1 + max(kafka_producer.partitions_for(topic))

    def consume(key, value):
        database = key['database']
        key_str = json.dumps(key)
        value_str = json.dumps(value)
        partition = abs(java_string_hashcode(database) % partition_count)
        kafka_producer.send(topic, key=key_str, value=value_str, partition=partition)

    return consume


def generate_console_consumer():
    def consume(key, value):
        print key, value
    return consume


def produce_messages(f_consume, args, config):
    seed = config['generator']['seed']
    producers = []

    # iterate all schema, database and table
    for schema in config['mysql']['schemas']:
        for database in config['mysql']['schemas'][schema]['databases']:
            for table in config['mysql']['schemas'][schema]['tables']:
                producers.extend(generate_producers_for_table(seed, schema, database, table, config))

    # Filter producer by arguments
    producers = filter(lambda x: args.schema is None or x.table.schema == args.schema, producers)
    if len(producers) == 0: usage('could not find specified schema')
    producers = filter(lambda x: args.database is None or x.table.database == args.database, producers)
    if len(producers) == 0: usage('could not find specified database')
    producers = filter(lambda x: args.table is None or x.table.table_name == args.table, producers)
    if len(producers) == 0: usage('could not find specified table')

    # Check lag and try produce every 10 ms
    timer = Timer()
    timer.start()
    while True:
        timer.tick()
        message_thunks = []
        for p in producers:
            message_thunks.extend(p.try_produce_thunks(timer.time_elapsed_ms))

        # sort thunks by timestamp, to ensure the output order
        message_thunks.sort()

        # generation message in time-order, evaluate thunks to get message and apply the consume function
        for order_key, f_produce_one in message_thunks:
            msg_key, msg_value = f_produce_one()
            f_consume(msg_key, msg_value)

        sleep(0.01)


def generate_producers_for_table(seed, schema, database, table_name, config):
    operation_desc = config['mysql']['schemas'][schema]['tables'][table_name][database]
    max_id = int(float(operation_desc['size']))
    row_gen = RowGenerator.get_instance(schema, database, table_name, config)
    table = Table(max_id, schema, database, table_name, seed, row_gen)
    producers = []

    if operation_desc['insert-rate'] is not None:
        insert_rate = parse_rate(operation_desc['insert-rate'])
        insert_producer = MessageProducer(table, insert_rate, "insert", 0)
        producers.append(insert_producer)

    if operation_desc['update-rate'] is not None:
        update_rate = parse_rate(operation_desc['update-rate'])
        update_producer = MessageProducer(table, update_rate, "update", 1)
        producers.append(update_producer)

    if operation_desc['delete-rate'] is not None:
        delete_rate = parse_rate(operation_desc['delete-rate'])
        delete_producer = MessageProducer(table, delete_rate, "delete", 2)
        producers.append(delete_producer)

    return producers


# parse the produce rate into num/ms
def parse_rate(rate_srt):
    num, time_frame = rate_srt.split('/')
    time_frame = time_frame.strip()
    num = float(num)
    if time_frame == 'second':
        rate = num / 1000.0
    elif time_frame == 'minute':
        rate = num / 1000.0 / 60.0
    elif time_frame == 'hour':
        rate = num / 1000.0 / 3600.0
    elif time_frame == 'day':
        rate = num / 1000.0 / 3600.0 / 24.0
    else:
        raise Exception('invalid duration')
    return rate


class Timer(object):
    def __init__(self):
        self.start_time_ms = 0
        self.time_elapsed_ms = 0

    def start(self):
        self.start_time_ms = time() * 1000.0

    def tick(self):
        self.time_elapsed_ms = time() * 1000.0 - self.start_time_ms


class Table(object):
    def __init__(self, max_id, schema, database, table_name, seed, row_gen):
        self.schema = schema
        self.database = database
        self.table_name = table_name
        self.max_id = max_id
        self.seed = seed
        self.row_gen = row_gen


class MessageProducer(object):
    def __init__(self, table, rate, operation, priority):
        self.table = table
        self.rate = rate
        self.operation = operation
        # priority of operation
        self.priority = priority
        self.produced_count = 0

    def try_produce_thunks(self, time_elapsed):
        should_have_produced = int(self.rate * time_elapsed)
        num_to_produce = should_have_produced - self.produced_count
        for i in range(0, num_to_produce):
            timestamp = (self.produced_count + i + 1) / self.rate
            order_key = (timestamp, self.priority)
            yield order_key, self.produce_one

    def produce_one(self):
        if self.operation == 'insert':
            # max id is the next row index, because id is 1 based, row index is 0 based
            row_idx = self.table.max_id
            self.table.max_id += 1
        else:
            # generate id for update and delete message
            row_idx = pseudorandom_long([self.table.seed, 'id', self.operation, self.produced_count], 0,
                                        self.table.max_id)

        self.produced_count += 1
        row_gen = self.table.row_gen
        data = row_gen.generate_row(row_idx)
        pk_name = row_gen.primary_key_field
        pk_value = row_gen.generate_primary_key(row_idx)
        return maxwell_message(self.table.database, self.table.table_name, self.operation, data, pk_name, pk_value)
