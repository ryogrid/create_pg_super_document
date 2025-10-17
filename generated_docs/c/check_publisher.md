# check_publisher

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:841-960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L841-L960)

## Overview
Validates that the primary server is properly configured and ready for logical replication by checking essential parameters and resource availability.

## Definition

```c
static void
check_publisher(const struct LogicalRepInfo *dbinfo)
```
## Detailed Description
This function performs comprehensive validation of the publisher (primary server) to ensure it can support logical replication. It verifies that the server is not in recovery mode and checks critical configuration parameters required for logical replication:

1. Confirms the server is not in recovery (cascading replication scenario)
2. Validates wal_level is set to 'logical'  
3. Ensures sufficient replication slots are available
4. Verifies adequate WAL sender processes are available
5. Checks max_prepared_transactions setting and issues warnings if needed

The function connects to the first database in the dbinfo array and executes a comprehensive query to gather all necessary configuration values in a single round trip.

## Parameters / Member Variables
- `*dbinfo`: Array of LogicalRepInfo structures containing database connection information, uses the first entry for publisher validation
## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - [connect_database](connect_database.md)
  - [server_is_in_recovery](../s/server_is_in_recovery.md)
  - [disconnect_database](../d/disconnect_database.md)
  - [PQexec](../P/PQexec.md)
  - PGRES_TUPLES_OK
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - atoi
  - pg_log_debug
  - pg_log_error
  - pg_log_error_hint
  - pg_log_warning
  - pg_log_warning_detail
  - [pg_free](../p/pg_free.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- Terminates the program if any validation fails
- Uses a single SQL query to fetch multiple configuration parameters for efficiency
- Provides specific recommendations for parameter adjustments when resources are insufficient
- Warns about two_phase option limitations when max_prepared_transactions > 0
- Critical prerequisite check before setting up logical replication infrastructure

## Simplified Source

```c
static void
check_publisher(const struct LogicalRepInfo *dbinfo)
{
    pg_log_info("checking settings on publisher");

    PGconn *conn = connect_database(dbinfo[0].pubconninfo, true);

    // Check if primary is in recovery (cascading scenario)
    if (server_is_in_recovery(conn))
    {
        pg_log_error("primary server cannot be in recovery");
        disconnect_database(conn, true);
    }

    // Query all required configuration parameters
    PGresult *res = PQexec(conn,
        "SELECT pg_catalog.current_setting('wal_level'),"
        " pg_catalog.current_setting('max_replication_slots'),"
        " (SELECT count(*) FROM pg_catalog.pg_replication_slots),"
        " pg_catalog.current_setting('max_wal_senders'),"
        " (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE backend_type = 'walsender'),"
        " pg_catalog.current_setting('max_prepared_transactions')");

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        pg_log_error("could not obtain publisher settings: %s", PQresultErrorMessage(res));
        disconnect_database(conn, true);
    }

    // Extract configuration values
    char *wal_level = pg_strdup(PQgetvalue(res, 0, 0));
    int max_repslots = atoi(PQgetvalue(res, 0, 1));
    int cur_repslots = atoi(PQgetvalue(res, 0, 2));
    int max_walsenders = atoi(PQgetvalue(res, 0, 3));
    int cur_walsenders = atoi(PQgetvalue(res, 0, 4));
    int max_prepared_transactions = atoi(PQgetvalue(res, 0, 5));

    PQclear(res);
    disconnect_database(conn, false);

    // Validate configuration
    bool failed = false;

    if (strcmp(wal_level, "logical") != 0)
    {
        pg_log_error("publisher requires wal_level >= \"logical\"");
        failed = true;
    }

    if (max_repslots - cur_repslots < num_dbs)
    {
        pg_log_error("publisher requires %d replication slots, but only %d remain",
                     num_dbs, max_repslots - cur_repslots);
        pg_log_error_hint("Increase the configuration parameter \"%s\" to at least %d.",
                         "max_replication_slots", cur_repslots + num_dbs);
        failed = true;
    }

    if (max_walsenders - cur_walsenders < num_dbs)
    {
        pg_log_error("publisher requires %d WAL sender processes, but only %d remain",
                     num_dbs, max_walsenders - cur_walsenders);
        pg_log_error_hint("Increase the configuration parameter \"%s\" to at least %d.",
                         "max_wal_senders", cur_walsenders + num_dbs);
        failed = true;
    }

    // Warning for prepared transactions
    if (max_prepared_transactions != 0)
    {
        pg_log_warning("two_phase option will not be enabled for replication slots");
        pg_log_warning_detail("Subscriptions will be created with the two_phase option disabled. "
                             "Prepared transactions will be replicated at COMMIT PREPARED.");
    }

    pg_free(wal_level);

    if (failed)
        exit(1);
}
```