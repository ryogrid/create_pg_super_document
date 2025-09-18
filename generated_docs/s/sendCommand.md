# sendCommand

## Location
src/bin/pgbench/pgbench.c: 3155 - 3207

## Overview
The sendCommand function sends a SQL command to the PostgreSQL server using the configured query mode (simple, extended, or prepared statements).

## Definition
```c
static bool sendCommand(CState *st, Command *command)
```

## Detailed Description
This function is responsible for dispatching SQL commands to the PostgreSQL server using one of three query modes supported by pgbench. The function adapts its behavior based on the global querymode setting:

1. **QUERY_SIMPLE**: Uses PQsendQuery with variable substitution in the SQL text
2. **QUERY_EXTENDED**: Uses PQsendQueryParams with parameterized queries  
3. **QUERY_PREPARED**: Uses PQsendQueryPrepared with prepared statements

For each mode, the function:
- Handles variable substitution appropriately (text replacement for simple mode, parameter binding for others)
- Logs debug information about the command being sent
- Uses the appropriate PostgreSQL libpq function to send the query asynchronously
- Returns success/failure status based on the libpq call result

The function ensures prepared statements are created when needed in QUERY_PREPARED mode by calling prepareCommand before execution.

## Parameters / Member Variables
- `st`: Pointer to CState structure representing the client connection state
- `command`: Pointer to Command structure containing the SQL command and its parameters

## Dependencies
- Functions called/Symbols referenced:
  - prepareCommand (for preparing statements in QUERY_PREPARED mode)
  - assignVariables (for variable substitution in simple mode)
  - getQueryParams (for parameter extraction in extended/prepared modes)
  - PQsendQuery, PQsendQueryParams, PQsendQueryPrepared (PostgreSQL libpq functions)
  - pg_log_debug (for debug logging)
  - QUERY_SIMPLE, QUERY_EXTENDED, QUERY_PREPARED (query mode constants)
  - MAX_ARGS (maximum parameter count)
- Called from (representative examples):
  - advanceConnectionState

## Notes and Other Information
- This function is central to pgbench's query execution mechanism
- The choice of query mode affects both performance characteristics and SQL syntax requirements
- Simple mode allows dynamic SQL but has parsing overhead
- Extended mode provides parameter safety and some performance benefits
- Prepared mode offers the best performance for repeated queries
- The function uses asynchronous libpq functions to enable non-blocking operation
- Error handling is minimal - failures are indicated by return value only
- Debug logging helps with troubleshooting query execution issues