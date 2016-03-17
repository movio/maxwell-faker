# -*- coding: utf-8 -*-

import json
import yaml
import argparse
from time import time

from config import validate_config
from utils import usage
from row_generator import RowGenerator

def produce(message):
    print message # FIXME: produce to Kafka

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

def bootstrap(schema, database, table, config):
    produce(bootstrap_start_message(database, table))
    for message in bootstrap_insert_messages(schema, database, table, config):
        produce(message)
    produce(bootstrap_complete_message(database, table))

def bootstrap_insert_messages(schema, database, table, config):
    count = int(float(config['mysql']['schemas'][schema]['tables'][table][database]['bootstrap-count']))
    row_generator = RowGenerator.get_instance(schema, database, table, config)
    for row_index in xrange(count):
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
    schema = find_schema(config, args.database, args.table)
    validate_config(config)
    try:
        bootstrap(schema, args.database, args.table, config)
    except IOError, e:
        usage(e)
    except KeyboardInterrupt:
        pass
