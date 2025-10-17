# setup_subscriber

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1143-1182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1143-L1182)

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
  - [connect_database](../c/connect_database.md) (establishes connection to subscriber database)
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

## Simplified Source

```c
static void
setup_subscriber(struct LogicalRepInfo *dbinfo, const char *consistent_lsn)
{
    // Process each database for logical replication setup
    for (int i = 0; i < num_dbs; i++) {
        PGconn *conn;

        // Connect to subscriber database
        conn = connect_database(dbinfo[i].subconninfo, true);

        // Clean up pre-existing subscriptions that could conflict
        check_and_drop_existing_subscriptions(conn, &dbinfo[i]);

        // Remove publications from subscriber (not needed on subscriber side)
        drop_publication(conn, &dbinfo[i]);

        // Create new subscription for logical replication
        create_subscription(conn, &dbinfo[i]);

        // Set replication starting point to consistent LSN
        set_replication_progress(conn, &dbinfo[i], consistent_lsn);

        // Enable the subscription to start replication
        enable_subscription(conn, &dbinfo[i]);

        // Clean up database connection
        disconnect_database(conn, false);
    }
}
```