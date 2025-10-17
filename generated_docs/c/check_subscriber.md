# check_subscriber

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:961-1061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L961-L1061)

## Overview
Validates that the standby server is properly configured and ready to become a logical replication subscriber by checking essential parameters and confirming recovery status.

## Definition

```c
static void
check_subscriber(const struct LogicalRepInfo *dbinfo)
```
## Detailed Description
This function performs comprehensive validation of the subscriber (standby server) to ensure it can support logical replication after promotion. It verifies critical prerequisites and configuration parameters:

1. Confirms the server is currently in recovery mode (must be a standby)
2. Validates sufficient replication slots are available for all databases
3. Ensures adequate logical replication workers are configured
4. Verifies sufficient worker processes are available (requires num_dbs + 1)
5. Extracts primary_slot_name if configured for physical replication

The function connects to the subscriber using the first database connection info and performs parameter validation to prevent runtime failures during logical replication setup.

## Parameters / Member Variables
- `*dbinfo`: Array of LogicalRepInfo structures containing database connection information, uses the first entry for subscriber validation
## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - [connect_database](connect_database.md)
  - [server_is_in_recovery](../s/server_is_in_recovery.md)
  - [disconnect_database](../d/disconnect_database.md)
  - [PQexec](../P/PQexec.md)
  - PGRES_TUPLES_OK
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - atoi
  - [PQgetvalue](../P/PQgetvalue.md)
  - strcmp
  - [pg_strdup](../p/pg_strdup.md)
  - pg_log_debug
  - [PQclear](../P/PQclear.md)
  - pg_log_error
  - pg_log_error_hint
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- Must be called before promoting the standby to ensure logical replication compatibility
- Terminates the program if validation fails
- Sets global primary_slot_name variable if physical replication slot is configured
- Worker process requirement includes one additional process beyond the number of databases
- Cannot reliably detect cascaded replication scenarios that would be broken by pg_resetwal
- Critical prerequisite check executed early in the conversion process

## Simplified Source

```c
static void
check_subscriber(const struct LogicalRepInfo *dbinfo)
{
    PGconn *conn;
    PGresult *res;
    bool failed = false;
    int max_lrworkers, max_repslots, max_wprocs;

    pg_log_info("checking settings on subscriber");

    // Connect to subscriber database
    conn = connect_database(dbinfo[0].subconninfo, true);

    // Verify server is in recovery mode (must be standby)
    if (!server_is_in_recovery(conn)) {
        pg_log_error("target server must be a standby");
        disconnect_database(conn, true);
    }

    // Query configuration parameters for logical replication
    res = PQexec(conn,
                 "SELECT setting FROM pg_catalog.pg_settings WHERE name IN ("
                 "'max_logical_replication_workers', "
                 "'max_replication_slots', "
                 "'max_worker_processes', "
                 "'primary_slot_name') "
                 "ORDER BY name");

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log_error("could not obtain subscriber settings: %s",
                     PQresultErrorMessage(res));
        disconnect_database(conn, true);
    }

    // Extract parameter values
    max_lrworkers = atoi(PQgetvalue(res, 0, 0));
    max_repslots = atoi(PQgetvalue(res, 1, 0));
    max_wprocs = atoi(PQgetvalue(res, 2, 0));
    if (strcmp(PQgetvalue(res, 3, 0), "") != 0)
        primary_slot_name = pg_strdup(PQgetvalue(res, 3, 0));

    PQclear(res);
    disconnect_database(conn, false);

    // Validate configuration parameters against requirements
    if (max_repslots < num_dbs) {
        pg_log_error("subscriber requires %d replication slots, but only %d remain",
                     num_dbs, max_repslots);
        failed = true;
    }

    if (max_lrworkers < num_dbs) {
        pg_log_error("subscriber requires %d logical replication workers, but only %d remain",
                     num_dbs, max_lrworkers);
        failed = true;
    }

    if (max_wprocs < num_dbs + 1) {
        pg_log_error("subscriber requires %d worker processes, but only %d remain",
                     num_dbs + 1, max_wprocs);
        failed = true;
    }

    if (failed)
        exit(1);
}
```