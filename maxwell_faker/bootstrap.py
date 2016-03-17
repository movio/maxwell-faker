# -*- coding: utf-8 -*-

import json
import yaml
import argparse
from time import time

from config import validate_config
from utils import usage
from row_generator import RowGenerator

def validate_config_for_bootstrap(args, config):
    if args.schema not in config['mysql']['schemas']:
        usage('unknown schema ' + args.schema)
    if args.table not in config['mysql']['tables']:
        usage('unknown table ' + args.table)

def produce(message):
    print message # FIXME: produce to Kafka

def maxwell_message(schema, table, type, data):
    return json.dumps({
        "database": schema,
        "table": table,
        "type": type,
        "ts": int(time()),
        "data": data
    })

def bootstrap_start_message(schema, table):
    return maxwell_message(schema, table, "bootstrap-start", {})

def bootstrap_complete_message(schema, table):
    return maxwell_message(schema, table, "bootstrap-complete", {})

def bootstrap(schema, table, config):
    produce(bootstrap_start_message(schema, table))
    for message in bootstrap_insert_messages(schema, table, config):
        produce(message)
    produce(bootstrap_complete_message(schema, table))

def bootstrap_insert_messages(schema, table, config):
    count = int(float(config['mysql']['tables'][table]['bootstrap-count']))
    row_generator = RowGenerator.get_instance(table, config)
    for row_index in xrange(count):
        yield maxwell_message(schema, table, "bootstrap-insert", row_generator.generate_row(row_index))

def main():
    parser = argparse.ArgumentParser(description='Generate fake data for Maxwell.')
    parser.add_argument('--config', metavar='CONFIG', type=str, required=True, help='path to yaml config file')
    parser.add_argument('--schema', metavar='SCHEMA', type=str, required=True, help='schema to bootstrap')
    parser.add_argument('--table', metavar='TABLE', type=str, required=True, help='table to bootstrap')
    args = parser.parse_args()
    config = yaml.load(open(args.config).read())
    validate_config(config)
    validate_config_for_bootstrap(args, config)
    bootstrap(args.schema, args.table, config)
