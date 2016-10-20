# -*- coding: utf-8 -*-

import json
import yaml
import argparse

from config import validate_config
from utils import usage, java_string_hashcode
from row_generator import RowGenerator


def maxwell_message(database, table, operation, data, pk_name, pk_value):
    key = {
        "database": database,
        "table": table.lower(),
        "pk." + pk_name: pk_value
    }

    value = {
        "database": database,
        "table": table.lower(),
        "type": operation,
        "ts": 0,
        "data": data
    }
    return key, value


def main():
    parser = argparse.ArgumentParser(description='Fake Maxwell data into Kafka.')
    parser.add_argument('--config', metavar='CONFIG', type=str, required=True, help='path to yaml config file')
    parser.add_argument('--schema', metavar='SCHEMA', type=str, required=True, help='schema to produce')
    parser.add_argument('--database', metavar='DATABASE', type=str, required=True, help='database to produce')
    parser.add_argument('--table', metavar='TABLE', type=str, required=True, help='table to generate')
    parser.add_argument('--id', metavar='ID', type=int, required=True, help='id to generate')
    parser.add_argument('--partition-count', metavar='PARTITION_COUNT', type=int, default=12, required=False,
                        help='number of partitions (default 12)')
    args = parser.parse_args()

    config = yaml.load(open(args.config).read())
    validate_config(config)
    f_consume = generate_console_consumer(args)

    resolve_ref(f_consume, args, config)


def generate_console_consumer(args):
    partition_count = args.partition_count

    def consume(key, value):
        partition = abs(java_string_hashcode(key['database']) % partition_count)
        output = {
            "partition": partition,
            "key": key,
            "message": value
        }
        print json.dumps(output, separators=(',',':'))

    return consume


def resolve_ref(f_consume, args, config):
    schema = args.schema
    database = args.database

    def resolve(table, row_id):
        row_id = int(row_id)
        row_gen = RowGenerator.get_instance(schema, database, table, config)
        row_idx = row_id - 1
        data = row_gen.generate_row(row_idx)
        pk_name = row_gen.primary_key_field
        pk_value = row_gen.generate_primary_key(row_idx)
        f_consume(*maxwell_message(database, table, 'insert', data, pk_name, pk_value))

        foreign_fields = filter(lambda x: x.type == 'foreign-key', row_gen.fields.values())
        for foreign_field in foreign_fields:
            foreign_table = foreign_field.options
            foreign_id = data.get(foreign_field.name)
            if foreign_id is not None:
                resolve(foreign_table, foreign_id)

    resolve(args.table, args.id)
