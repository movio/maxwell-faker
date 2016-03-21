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
    parser.add_argument('--database', metavar='DATABASE', type=str, required=False, help='database to produce')
    parser.add_argument('--table', metavar='TABLE', type=str, required=False, help='table to produce')
    args = parser.parse_args()

    config = yaml.load(open(args.config).read())
    validate_config(config)

    kafka_producer = KafkaProducer(bootstrap_servers=config['kafka']['brokers'])
    try:

        produce_func = generate_produce_func(config, kafka_producer)
        produce_messages(produce_func, args, config)
    except IOError, e:
        usage(e)
    except KeyboardInterrupt:
        kafka_producer.flush()
        kafka_producer.close()
        sys.exit(1)


def generate_produce_func(config, kafka_producer):
    topic = config['kafka']['topic']
    partition_count = 1 + max(kafka_producer.partitions_for(topic))

    def produce(key, value):
        database = key['database']
        key_str = json.dumps(key)
        value_str = json.dumps(value)
        partition = abs(java_string_hashcode(database) % partition_count)
        kafka_producer.send(topic, key=key_str, value=value_str, partition=partition)

    return produce


def produce_messages(produce_func, args, config):
    seed = config['generator']['seed']
    producers = []

    # iterate all schema, database and table
    for schema in config['mysql']['schemas']:
        for database in config['mysql']['schemas'][schema]['databases']:
            for table in config['mysql']['schemas'][schema]['tables']:
                row_generator = RowGenerator.get_instance(schema, database, table, config)
                producers.extend(generate_producers_for_table(produce_func, seed, schema, database, table, config,
                                                              row_generator))

    # Filter producer by arguments
    producers = filter(lambda x: args.database is None or x.table.database == args.database, producers)
    if len(producers) == 0: usage('could not find specified database')
    producers = filter(lambda x: args.table is None or x.table.table_name == args.table, producers)
    if len(producers) == 0: usage('could not find specified table')

    # Check lag and try produce every 10 ms
    while True:
        for p in producers:
            p.try_produce()
        sleep(0.01)


def generate_producers_for_table(produce_func, seed, schema, database, table_name, config, row_gen):
    operation_desc = config['mysql']['schemas'][schema]['tables'][table_name][database]
    max_id = int(float(operation_desc['bootstrap-count']))
    table = Table(max_id, schema, database, table_name, seed, row_gen)
    producers = []

    if operation_desc['insert-rate'] is not None:
        insert_rate = parse_rate(operation_desc['insert-rate'])
        insert_producer = MessageProducer(produce_func, table, insert_rate, "insert")
        producers.append(insert_producer)

    if operation_desc['update-rate'] is not None:
        update_rate = parse_rate(operation_desc['update-rate'])
        update_producer = MessageProducer(produce_func, table, update_rate, "update")
        producers.append(update_producer)

    if operation_desc['delete-rate'] is not None:
        delete_rate = parse_rate(operation_desc['delete-rate'])
        delete_producer = MessageProducer(produce_func, table, delete_rate, "delete")
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


class Table(object):
    def __init__(self, max_id, schema, database, table_name, seed, row_gen):
        self.schema = schema
        self.database = database
        self.table_name = table_name
        self.max_id = max_id
        self.seed = seed
        self.row_gen = row_gen


class MessageProducer(object):
    def __init__(self, produce_func, table, rate, operation):
        self.produce_func = produce_func
        self.table = table
        self.rate = rate
        self.operation = operation
        self.start_time = time() * 1000.0
        self.produced_count = 0

    def try_produce(self):
        time_elapsed = time() * 1000.0 - self.start_time
        should_have_produced = int(self.rate * time_elapsed)
        num_to_produce = should_have_produced - self.produced_count
        for i in range(0, num_to_produce):
            self.produce_one()

    def produce_one(self):
        if self.operation == 'insert':
            self.table.max_id += 1
            self.produced_count += 1
            row_idx = self.table.max_id
        else:
            self.produced_count += 1
            # generate id for update and delete message
            row_idx = pseudorandom_long([self.table.seed, 'id', self.operation, self.produced_count], 0,
                                        self.table.max_id)
        row_gen = self.table.row_gen
        data = row_gen.generate_row(row_idx)
        pk_name = row_gen.primary_key_field
        pk_value = row_gen.generate_primary_key(row_idx)
        self.produce_func(*maxwell_message(self.table.database, self.table.table_name, self.operation, data,
                                           pk_name, pk_value))
