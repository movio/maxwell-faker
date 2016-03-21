Maxwell Faker
=============

[Maxwell](https://github.com/zendesk/maxwell) is a really useful MySQL binlog to Kafka replicator built by [Zendesk](https://www.zendesk.com/).  
*Maxwell Faker* generates deterministic pseudorandom data and writes it to Kafka, in the same JSON format at Maxwell.
Maxwell Faker is useful in staging environments, systems tests, or load tests.

# Features

* bootstrapping (similar to Maxwell's `maxwell-bootstrap` utility)
* multiple MySQL schemas
* multiple databases per schema
* configurable database size and change rate per database  
* deterministic pseudorandom data generation with a configurable seed 

# Limitations

* databases with multiple primary keys are not supported

# YAML Configuration Format

Maxwell Faker is configured through a single YAML file. See [example.yaml](https://github.com/movio/maxwell-faker/blob/master/example.yaml) for a full example

| Key                                                                               | Notes  |
| --------------------------------------------------------------------------------- | ------ |
| `generator.seed`                                                                  | seed for the pseudorandom generator |
| `kafka.brokers`                                                                   | list of brokers in HOST:PORT format |
| `kafka.topic`                                                                     | Kafka topic to produce messages to |
| `mysql.schemas.<schema>.databases`                                           | list of databases for the specified schema |
| `mysql.schemas.<schema>.tables.<table>.<db>.size` | number of rows to insert when bootstrapping |
| `mysql.schemas.<schema>.tables.<table>.<db>.insert-rate`     | insert rate of the specified table |
| `mysql.schemas.<schema>.tables.<table>.<db>.update-rate`     | update rate of the specified table |
| `mysql.schemas.<schema>.tables.<table>.<db>.delete-rate`     | delete rate of the specified table |
| `mysql.schemas.<schema>.tables.<table>.template.<column>`          | column definition for the specified column (see below) |


# Column Definition Syntax


# Usage
