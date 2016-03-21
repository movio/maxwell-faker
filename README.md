Maxwell Faker
=============

[Maxwell](https://github.com/zendesk/maxwell) is a really useful MySQL binlog to Kafka replicator built by [Zendesk](https://www.zendesk.com/).  
**Maxwell Faker** generates deterministic pseudorandom data and writes it to Kafka, in the same JSON format as Maxwell.
Maxwell Faker is useful in staging environments, systems tests, or load tests.

## Features

* bootstrapping (similar to Maxwell's `maxwell-bootstrap` utility)
* multiple MySQL schemas
* multiple databases per schema
* configurable database size and change rate per database  
* deterministic pseudorandom data generation with a configurable seed 

## Limitations

* tables with compound primary keys are not supported
* producing is Kafka-only for now
* currently slower than Maxwell
* row updates are currently idempotent

## Installation

`# pip install .`

## YAML Configuration Format

Maxwell Faker is configured through a single YAML file. See [example.yaml](https://github.com/movio/maxwell-faker/blob/master/example.yaml) for a full example

| Key                                                          | Notes  |
| -------------------------------------------------------------| ------ |
| `generator.seed`                                             | seed for the pseudorandom generator |
| `kafka.brokers`                                              | list of brokers in HOST:PORT format |
| `kafka.topic`                                                | Kafka topic to produce messages to |
| `mysql.schemas.<schema>.databases`                           | list of databases for the specified schema |
| `mysql.schemas.<schema>.tables.<table>.<db>.size`            | number of rows to insert when bootstrapping |
| `mysql.schemas.<schema>.tables.<table>.<db>.insert-rate`     | insert rate of the specified table (see below) |
| `mysql.schemas.<schema>.tables.<table>.<db>.update-rate`     | update rate of the specified table (see below) |
| `mysql.schemas.<schema>.tables.<table>.<db>.delete-rate`     | delete rate of the specified table (see below) |
| `mysql.schemas.<schema>.tables.<table>.template.<column>`    | column definition (see below) |

## Column Definition Syntax

The syntax for column definition is `TYPE{[OPTIONS]}{?}`.
That is a type identifier, optionally followed by options between square brackets, optionally followed by a question mark to denote a nullable column.

The supported types and options are as follows:

| Type      | Options  | Notes |
| --------- | -------- | ----- |
| integer   | min, max | pseudorandom integer between min (incl.) and max (excl.) |
| float     | min, max | pseudorandom float between min (incl.) and max (excl.) |
| string    | min, max | pseudorandom string of length between min (incl.) and max (excl.) |
| date      | (none) | Date in `YYYY-MM-DD` format |
| date-time | (none) | Date-time in `YYYY-MM-DD hh:mm:ss` format |
| enum      | value1, value2, ... | enumeration of the specified values |
| foreign-key | table-name | will generate a valid, existing, foreign key |

All column definitions can be suffixed by a `?` to denote a nullable column.

## Message Rate Syntax

The syntax for insert, update and delete rates is `NUMBER / DURATION` where duration should be one of
`second`, `minute`, `hour` or `day`.

## Usage

Use `maxwell-faker-bootstrap` to bootstrap a table:

`$ maxwell-faker-bootstrap --config example.yaml --database store_records_initech --table customers`

Use `maxwell-faker` to continuously generate pseudorandom data.

`$ maxwell-faker --config example.yaml`

 By default, `maxwell-faker` will produce data for all schema, all databases, and all tables.
 Use the `--schema`, `--database`, or `--table` flags to optionally filter the output.

