# drop_replication_slot

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1373-1412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1373-L1412)

## Overview
A utility function that drops a logical replication slot from a PostgreSQL database connection, primarily used in the pg_createsubscriber tool for cleanup operations.

## Definition
static void drop_replication_slot(PGconn *conn, struct LogicalRepInfo *dbinfo, const char *slot_name)

## Detailed Description
This function provides a safe way to drop replication slots during the pg_createsubscriber process. It constructs and executes a SQL command to drop the specified replication slot using the pg_drop_replication_slot() catalog function.

Key functionality includes:
1. **Input Validation**: Ensures the connection is not NULL
2. **SQL Injection Protection**: Uses PQescapeLiteral() to safely escape the slot name
3. **Logging**: Provides informational and debug logging for the operation
4. **Dry Run Support**: Respects the global dry_run flag, allowing testing without actual execution
5. **Error Handling**: Logs errors and updates the dbinfo state to prevent retry attempts

The function is designed to be resilient - if dropping fails, it marks the slot as not created (made_replslot = false) to prevent further attempts, avoiding infinite retry loops.

## Parameters / Member Variables
- : Active PostgreSQL database connection to execute the drop command on
- : Pointer to LogicalRepInfo structure containing database information and state
- : Name of the replication slot to drop (will be properly escaped before use)

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md), destroyPQExpBuffer, appendPQExpBuffer (PQExpBuffer utilities)
  - [PQescapeLiteral](../P/PQescapeLiteral.md), PQfreemem (libpq string escaping)
  - [PQexec](../P/PQexec.md), PQresultStatus, PQresultErrorMessage, PQclear (libpq query execution)
  - pg_log_info, pg_log_debug, pg_log_error (logging functions)
  - PGRES_TUPLES_OK (result status constant)

- Called from (representative examples):
  - [cleanup_objects_atexit](../c/cleanup_objects_atexit.md)
  - [drop_primary_replication_slot](drop_primary_replication_slot.md)
  - [drop_failover_replication_slots](drop_failover_replication_slots.md)

## Notes and Other Information
- This is a static function specific to the pg_createsubscriber utility
- The function respects the global dry_run variable for testing purposes
- Uses pg_catalog.pg_drop_replication_slot() internally, which is the standard PostgreSQL function for dropping replication slots
- Error handling includes updating the LogicalRepInfo state to prevent retry attempts
- Proper memory management with PQfreemem() and destroyPQExpBuffer()
- Part of the pg_createsubscriber tool's cleanup and error recovery mechanisms