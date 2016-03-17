# -*- coding: utf-8 -*-

from utils import usage

def check(message, predicate):
    if not predicate:
        usage(message)

def validate_config(config):

    # generator section
    check(
        'section "generator" is missing',
        'generator' in config
    )
    check(
        'value "generator.seed" is missing',
        'seed' in config['generator']
    )

    # kafka section
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
