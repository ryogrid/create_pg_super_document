# drop_failover_replication_slots

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1282 - 1323

## Overview
Drops failover replication slots on the subscriber server that become unnecessary after the standby-to-subscriber transformation.

## Definition
```c
static void drop_failover_replication_slots(struct LogicalRepInfo *dbinfo)
```

## Detailed Description
This function performs cleanup by identifying and removing failover replication slots on the newly converted subscriber. These slots were used for high-availability failover scenarios in the standby configuration but are no longer needed after the server becomes a logical subscriber. The function queries the pg_replication_slots catalog to find all slots marked with the failover attribute, then removes each one. It implements graceful error handling - if connections fail or queries error, it provides warnings and hints for manual cleanup rather than aborting the entire conversion process.

## Parameters / Member Variables
- `dbinfo`: Array of LogicalRepInfo structures containing database and connection information (uses the first element for subscriber connection info)

## Dependencies
- Functions called/Symbols referenced:
  - connect_database (connects to subscriber server, with non-fatal connection flag)
  - [PQexec](../P/PQexec.md) (executes query to find failover replication slots)
  - [PQresultStatus](../P/PQresultStatus.md)/PGRES_TUPLES_OK (checks query execution success)
  - [PQntuples](../P/PQntuples.md) (gets number of result rows)
  - [PQgetvalue](../P/PQgetvalue.md) (retrieves slot names from query results)
  - [drop_replication_slot](drop_replication_slot.md) (removes individual replication slots)
  - [PQclear](../P/PQclear.md) (frees query result memory)
  - [disconnect_database](disconnect_database.md) (closes database connection)
  - pg_log_warning (logs warning messages for failures)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md) (gets error details from failed queries)
  - pg_log_warning_hint (provides helpful hints for manual cleanup)
- Called from:
  - [main](../m/main.md) (primary entry point of pg_createsubscriber utility)

## Notes and Other Information
- This is a static function, only accessible within pg_createsubscriber.c
- Designed to be non-fatal - failures result in warnings rather than errors
- Targets specifically failover-enabled replication slots using WHERE failover clause
- Critical for preventing unnecessary WAL file retention on the subscriber
- Part of the cleanup phase in the standby-to-subscriber conversion workflow
- Provides comprehensive error handling for both connection and query failures
- Uses non-fatal connection mode to gracefully handle subscriber unavailability
- Offers user guidance through warning hints if automatic cleanup fails