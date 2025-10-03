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