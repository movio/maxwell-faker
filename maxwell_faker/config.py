# -*- coding: utf-8 -*-

import re

from utils import usage

KAFKA_BROKER_CONNECTION_RE = re.compile('^.+:\d+$')
FIELD_SPECIFICATION_RE = re.compile('^([^\[\?]+)(\[.*\])?(\?)?$')
RATE_RE = re.compile('^\d+(.\d+)?\s*/\s*(second|minute|hour|day)$')

class Field(object):

    def __init__(self, name, specification):
        match = FIELD_SPECIFICATION_RE.match(specification)
        if match is None: usage('invalid field specification: ' + specification)
        field_type, field_options, field_optional = match.group(1), match.group(2), match.group(3)
        if field_options is not None: field_options = field_options[1:-1]
        self.name = name
        self.optional = field_optional
        self.options = field_options
        self.type = field_type


def check(message, predicate):
    if not predicate:
        usage(message)

def validate_config(config):
    validate_generator_section(config)
    validate_kafka_section(config)
    validate_mysql_section(config)

def validate_generator_section(config):
    check(
        'section "generator" is missing',
        'generator' in config
    )
    check(
        'value "generator.seed" is missing',
        'seed' in config['generator']
    )


def validate_kafka_section(config):
    check(
        'section "kafka" is missing',
        'kafka' in config
    )
    check(
        'section "kafka.brokers" is missing',
        'brokers' in config['kafka']
    )
    check(
        'section "kafka.brokers" should be a list',
        type(config['kafka']['brokers']) is list
    )
    check(
        'section "kafka.topic" is missing',
        'topic' in config['kafka']
    )
    for item in config['kafka']['brokers']:
        check(
            'items in section "kafka.brokers" must be of format host:port',
            KAFKA_BROKER_CONNECTION_RE.match(item)
        )


def validate_mysql_section(config):
    check(
        'section "mysql" is missing',
        'mysql' in config
    )
    check(
        'section "mysql.schemas" is missing',
        'schemas' in config['mysql']
    )
    for schema in config['mysql']['schemas']:
        validate_mysql_schema_section(config, schema)


def validate_mysql_schema_section(config, schema):
    check(
        'section "mysql.schemas.<schema>.databases" is missing',
        'databases' in config['mysql']['schemas'][schema]
    )
    check(
        'section "mysql.schemas.<schema>.databases" should be a list',
        type(config['mysql']['schemas'][schema]['databases'] is list)
    )
    check(
        'section "mysql.schemas.<schema>.tables" is missing ',
        'tables' in config['mysql']['schemas'][schema]
    )
    check(
        'section "mysql.schemas.<schema>.tables" should be a mapping',
        type(config['mysql']['schemas'][schema]['tables'] is dict)
    )
    for table in config['mysql']['schemas'][schema]['tables']:
        validate_mysql_table_section(config, schema, table)


def validate_mysql_table_section(config, schema, table):
    section = config['mysql']['schemas'][schema]['tables'][table]
    for database in config['mysql']['schemas'][schema]['databases']:
        check(
            'section "mysql.schemas.<schema>.tables.<table>.<database>" is missing',
            database in section
        )
        validate_database_table_section(config, schema, table, database)
    check(
        'table section is missing "template"',
        'template' in section
    )
    validate_table_template_section(config, schema, table)


def validate_database_table_section(config, schema, table, database):
    section = config['mysql']['schemas'][schema]['tables'][table]
    check(
        'database table section is missing "bootstrap-count"',
        'bootstrap-count' in section[database]
    )
    check(
        'database table section is missing "insert-rate"',
        'insert-rate' in section[database]
    )
    check(
        'database table section is missing "update-rate"',
        'update-rate' in section[database]
    )
    check(
        'database table section is missing "delete-rate"',
        'delete-rate' in section[database]
    )
    check(
        '"insert-rate" must be of format NUMBER / [second|minute|hour|day]',
        RATE_RE.match(section[database]['insert-rate'])
    )
    check(
        '"update-rate" must be of format NUMBER / [second|minute|hour|day]',
        RATE_RE.match(section[database]['update-rate'])
    )
    check(
        '"delete-rate" must be of format NUMBER / [second|minute|hour|day]',
        RATE_RE.match(section[database]['delete-rate'])
    )

def validate_table_template_section(config, schema, table):
    section = config['mysql']['schemas'][schema]['tables'][table]['template']
    for field in section:
        specification = config['mysql']['schemas'][schema]['tables'][table]['template'][field]
        validate_table_template_field_options(field, specification)

def validate_table_template_field_options(field_name, specification):
    check(
        'table field specification must be of format <field_type>[<options>]{?}',
        FIELD_SPECIFICATION_RE.match(specification)
    )
    field = Field(field_name, specification)
    check(
        'field_type must be one of [integer, float, string, date, date-time, enum, foreign-key]',
        field.type in [
            'integer',
            'float',
            'string',
            'date',
            'date-time',
            'enum',
            'foreign-key'
        ]
    )
