# discardUntilSync

## Location
[src/bin/pgbench/pgbench.c:3474-3526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3474-L3526)

## Overview
Handles pipeline synchronization and cleanup in pgbench by discarding query results until reaching a sync point, then exiting pipeline mode.

## Definition
static int discardUntilSync(CState *st)

## Detailed Description
This function is a critical error recovery mechanism in pgbench's pipeline mode operations. When a transaction fails or needs to be rolled back, this function ensures proper cleanup by:

1. Sending a pipeline sync command to the PostgreSQL server
2. Consuming and discarding all pending query results until a PGRES_PIPELINE_SYNC result is received
3. Resetting the sync counter to maintain accurate pipeline state
4. Exiting pipeline mode to prepare for subsequent operations

The function implements PostgreSQL's pipeline protocol requirements where PGRES_PIPELINE_SYNC results must be properly consumed and followed by a NULL result before the pipeline can be safely terminated.

## Parameters / Member Variables
- : Pointer to CState structure representing the client connection state, containing the database connection handle and synchronization counters

## Dependencies
- Functions called/Symbols referenced:
  - [PQpipelineSync](../P/PQpipelineSync.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [PQexitPipelineMode](../P/PQexitPipelineMode.md)
  - [PQclear](../P/PQclear.md)
  - pg_log_error
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md)

## Notes and Other Information
- This function is part of pgbench's transaction rollback and error recovery mechanism
- Critical for maintaining proper pipeline protocol compliance with PostgreSQL
- Returns 0 on failure, 1 on success
- Resets the num_syncs counter in the client state to 0 after successful sync
- Uses assertions to verify protocol compliance (PGRES_PIPELINE_SYNC followed by NULL)
- Essential for preventing connection state corruption when transactions fail in pipeline mode