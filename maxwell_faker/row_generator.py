# -*- coding: utf-8 -*-

import json
import re

from datetime import datetime

from utils import usage, pseudorandom_float, pseudorandom_long, pseudorandom_string
from config import Field


class RowGenerator(object):

    @staticmethod
    def get_instance(schema, database, table, config, instances = {}):
        key = (schema, database, table)
        if key not in instances:
            instances[key] = RowGenerator(schema, database, table, config, using_get_instance = True)
        return instances[key]

    def __init__(self, schema, database, table, config, using_get_instance = False):
        if not using_get_instance: raise ValueError("Use RowGenerator.get_instance to get RowGenerator instances")
        self.schema = schema
        self.database = database
        self.table = table
        self.config = config
        self.template = config['mysql']['schemas'][schema]['tables'][table]['template']
        self.fields = { key: Field(key, self.template[key]) for key in self.template }
        self.specifier = (schema, database, table, config['generator']['seed'])
        self.field_generators = {
            'integer': self.generate_integer_field,
            'float': self.generate_float_field,
            'foreign-key': self.generate_foreign_key,
            'date-time': self.generate_date_time,
            'date': self.generate_date,
            'enum': self.generate_enum,
            'string': self.generate_string
        }
        for field in self.fields:
            if self.fields[field].options == 'primary-key':
                self.primary_key_field = field

    def generate_field(self, field, row_index):
        field_specification = self.fields[field]
        if field_specification.optional:
            should_generate = pseudorandom_long([self.specifier, field, 'optional', row_index], 2)
            if not should_generate: return None
        return self.field_generators[field_specification.type](row_index, field, field_specification.options)

    def generate_integer_field(self, row_index, field, field_options):
        if field_options == "primary-key":
            return 1 + row_index
        else:
            lower, upper = field_options.split(',')
            return pseudorandom_long([self.specifier, field, row_index], int(lower), int(upper))

    def generate_float_field(self, row_index, field, field_options):
        lower, upper = field_options.split(',')
        return pseudorandom_float([self.specifier, field, row_index], float(lower), float(upper))

    def generate_foreign_key(self, row_index, field, field_options):
        foreign_table = field_options
        foreign_row_generator = RowGenerator.get_instance(self.schema, self.database, foreign_table, self.config)
        foreign_table_bootstrap_count = int(float(self.config['mysql']['schemas'][self.schema]['tables'][foreign_table][self.database]['size']))
        foreign_row_index = pseudorandom_long([self.specifier, field, row_index], foreign_table_bootstrap_count)
        return foreign_row_generator.generate_primary_key(foreign_row_index)

    def generate_date_time(self, row_index, field, field_options):
        lower, upper = 1142557409, 1773709409
        epoch = pseudorandom_long([self.specifier, field, row_index], lower, upper)
        format = "%Y-%m-%d %H:%M:%S"
        return datetime.fromtimestamp(epoch).strftime(format)

    def generate_enum(self, row_index, field, field_options):
        values = field_options.split(',')
        return values[pseudorandom_long([self.specifier, field, row_index], 0, len(values))]

    def generate_date(self, row_index, field, field_options):
        return self.generate_date_time(row_index, field, field_options).split()[0]

    def generate_string(self, row_index, field, field_options):
        if field_options == 'primary-key':
            if self.table.lower().endswith('s'):
                prefix = self.table[:-1]
            else:
                prefix = self.table
            return "%s_%d" % (prefix.upper(), row_index)
        lower, upper = field_options.split(',')
        return pseudorandom_string([self.specifier, field, row_index], int(lower), int(upper))

    def generate_primary_key(self, row_index):
        return self.generate_field(self.primary_key_field, row_index)

    def generate_row(self, row_index):
        row = {}
        for field in self.fields:
            value = self.generate_field(field, row_index)
            if value is not None:
                row[field] = value
        return row
