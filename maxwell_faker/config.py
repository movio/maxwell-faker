# -*- coding: utf-8 -*-

import re

from utils import usage

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
            re.compile('^.+:\d+$').match(item)
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
        'section "mysql.schemas.<schema>.databases" should be a list',
        type(config['mysql']['schemas'][schema]['databases'] is list)
    )
