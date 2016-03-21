# -*- coding: utf-8 -*-

import re
import sys

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


def check(message, predicate, path = None):
    if not predicate:
        if path:
            sys.stderr.write('configuration error at: ' + path + '\n\n' + message + '\n')
        else:
            sys.stderr.write('configuration error: ' + message + '\n')
        sys.exit(1)

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
        'value "seed" is missing',
        'seed' in config['generator'],
        'generator'
    )


def validate_kafka_section(config):
    check(
        'section "kafka" is missing',
        'kafka' in config
    )
    check(
        'section "brokers" is missing',
        'brokers' in config['kafka'],
        'kafka'
    )
    check(
        'section "brokers" should be a list',
        type(config['kafka']['brokers']) is list,
        'kafka'
    )
    check(
        'section "topic" is missing',
        'topic' in config['kafka'],
        'kafka'
    )
    for item in config['kafka']['brokers']:
        check(
            'brokers must be of format HOST:PORT',
            KAFKA_BROKER_CONNECTION_RE.match(item),
            'kafka.brokers'
        )


def validate_mysql_section(config):
    check(
        'section "mysql" is missing',
        'mysql' in config
    )
    check(
        'section "schemas" is missing',
        'schemas' in config['mysql'],
        'mysql'
    )
    for schema in config['mysql']['schemas']:
        validate_mysql_schema_section(config, "mysql.schemas." + schema, schema)


def validate_mysql_schema_section(config, path, schema):
    check(
        'section "databases" is missing',
        'databases' in config['mysql']['schemas'][schema],
        path
    )
    check(
        'section "databases" should be a list',
        type(config['mysql']['schemas'][schema]['databases'] is list),
        path
    )
    check(
        'section "tables" is missing ',
        'tables' in config['mysql']['schemas'][schema],
        path
    )
    check(
        'section "tables" should be a mapping',
        type(config['mysql']['schemas'][schema]['tables'] is dict),
        path
    )
    for table in config['mysql']['schemas'][schema]['tables']:
        validate_mysql_table_section(config, path + '.tables.' + table, schema, table)


def validate_mysql_table_section(config, path, schema, table):
    section = config['mysql']['schemas'][schema]['tables'][table]
    for database in config['mysql']['schemas'][schema]['databases']:
        check(
            'section "%s" is missing' % database,
            database in section,
            path
        )
        validate_database_table_section(config, path + '.' + database, schema, table, database)
    check(
        'section is missing "template"',
        'template' in section,
        path
    )
    validate_table_template_section(config, path, schema, table)


def validate_database_table_section(config, path, schema, table, database):
    section = config['mysql']['schemas'][schema]['tables'][table]
    check(
        'section is missing "size"',
        'size' in section[database],
        path
    )
    check(
        'section is missing "insert-rate"',
        'insert-rate' in section[database],
        path
    )
    check(
        'section is missing "update-rate"',
        'update-rate' in section[database],
        path
    )
    check(
        'section is missing "delete-rate"',
        'delete-rate' in section[database],
        path
    )
    check(
        '"insert-rate" must be of format NUMBER / [second|minute|hour|day]',
        RATE_RE.match(section[database]['insert-rate']),
        path
    )
    check(
        '"update-rate" must be of format NUMBER / [second|minute|hour|day]',
        RATE_RE.match(section[database]['update-rate']),
        path
    )
    check(
        '"delete-rate" must be of format NUMBER / [second|minute|hour|day]',
        RATE_RE.match(section[database]['delete-rate']),
        path
    )

def validate_table_template_section(config, path, schema, table):
    section = config['mysql']['schemas'][schema]['tables'][table]['template']
    for field in section:
        specification = config['mysql']['schemas'][schema]['tables'][table]['template'][field]
        validate_table_template_field_options(path + '.template.' + field, field, specification)

def validate_table_template_field_options(path, field_name, specification):
    check(
        'table field specification must be of format <field_type>[<options>]{?}',
        FIELD_SPECIFICATION_RE.match(specification),
        path
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
        ],
        path
    )
