# -*- coding: utf-8 -*-

import json
import re

from datetime import datetime

from utils import usage, fake_random_float, fake_random_int

FIELD_SPECIFICATION = re.compile('([^\[\?]+)(\[.*\])?(\?)?')

class RowGenerator(object):

    def __init__(self, table, config):
        self.table = table
        self.config = config
        self.template = config['mysql']['tables'][table]['template']
        self.seed = config['generator']['seed']

    def fields(self):
        return self.template.keys()

    def generate_field(self, field, id):
        field_specification = self.config['mysql']['tables'][self.table]['template'][field]
        match = FIELD_SPECIFICATION.match(field_specification)
        if match is None: usage('invalid field specification: ' + field_specification)
        field_type, field_options, field_optional = match.group(1), match.group(2), match.group(3)
        if field_options is not None: field_options = field_options[1:-1]
        if field_optional:
            should_generate = fake_random_int(self.seed, [field, 'optional'], id, 2)
            if not should_generate: return None
        return {
            'integer': self.generate_integer_field,
            'float': self.generate_float_field,
            'foreign-key': self.generate_foreign_key,
            'date-time': self.generate_date_time,
            'enum': self.generate_enum
        }[field_type](id, field, field_options)

    def generate_integer_field(self, id, field, field_options):
        if field_options == "primary-key":
            return id
        else:
            lower, upper = field_options.split(',')
            return fake_random_int(self.seed, field, id, int(lower), int(upper))

    def generate_float_field(self, id, field, field_options):
        lower, upper = field_options.split(',')
        return fake_random_float(self.seed, field, id, float(lower), float(upper))

    def generate_foreign_key(self, id, field, field_options):
        pass

    def generate_date_time(self, id, field, field_options):
        lower, upper = 1142557409, 1773709409
        epoch = fake_random_int(self.seed, field, id, lower, upper)
        format = "%Y-%m-%d %H:%M:%S"
        return datetime.fromtimestamp(epoch).strftime(format)

    def generate_enum(self, id, field, field_options):
        pass

    def generate_row(self, id):
        row = {}
        for field in self.fields():
            value = self.generate_field(field, id)
            if value is not None:
                row[field] = value
        return row
