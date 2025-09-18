# setup_subscriber

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1143 - 1182

## Overview
Creates and configures logical replication subscriptions on the newly formed subscriber, completing the transformation from physical standby to logical replica.

## Definition
```c
static void setup_subscriber(struct LogicalRepInfo *dbinfo, const char *consistent_lsn)
```

## Detailed Description
This function is the final step in the pg_createsubscriber process that converts a PostgreSQL standby server into a logical subscriber. It iterates through all databases and performs the complete subscription setup workflow: cleaning up pre-existing subscriptions and publications, creating new subscriptions, setting the correct replication progress to the consistent LSN, and enabling the subscriptions. This ensures that logical replication begins from the appropriate point without data inconsistencies.

## Parameters / Member Variables
- `dbinfo`: Array of LogicalRepInfo structures containing database and replication configuration information for each database
- `consistent_lsn`: String representation of the Log Sequence Number from which logical replication should begin, ensuring data consistency

## Dependencies
- Functions called/Symbols referenced:
  - connect_database (establishes connection to subscriber database)
  - [check_and_drop_existing_subscriptions](../c/check_and_drop_existing_subscriptions.md) (removes pre-existing subscriptions)
  - [drop_publication](../d/drop_publication.md) (removes publications from subscriber)
  - [create_subscription](../c/create_subscription.md) (creates new logical replication subscription)
  - [set_replication_progress](set_replication_progress.md) (sets subscription's LSN starting point)
  - [enable_subscription](../e/enable_subscription.md) (activates the subscription)
  - [disconnect_database](../d/disconnect_database.md) (closes database connection)
- Called from:
  - [main](../m/main.md) (primary entry point of pg_createsubscriber utility)

## Notes and Other Information
- This is a static function, only accessible within pg_createsubscriber.c
- Processes all databases in the global num_dbs array
- Performs proper connection management with connect/disconnect for each database
- Critical for maintaining data consistency by setting correct LSN starting point
- Part of the logical replication conversion workflow in PostgreSQL
- Ensures clean state by removing conflicting publications and subscriptions before setup