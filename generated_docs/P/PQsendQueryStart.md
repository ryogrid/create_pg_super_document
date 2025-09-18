# PQsendQueryStart

## Location
src/interfaces/libpq/fe-exec.c: 1673 - 1756

## Overview
PQsendQueryStart is a static function that provides common startup validation and state preparation for all PostgreSQL query sending functions.

## Definition


## Detailed Description
PQsendQueryStart serves as the foundation for all query sending operations in libpq, performing essential connection state validation and preparation before any query can be sent to the PostgreSQL server. This function implements the common logic shared across PQsendQuery, PQsendQueryParams, PQsendPrepare, and PQsendQueryPrepared.

The function handles different operational modes including normal query execution and pipeline mode operations. It validates that the connection is in an appropriate state for sending queries, manages error state clearing for new query cycles, and handles the complex state machine requirements for pipeline mode operations. The function ensures that queries can only be sent when the connection is ready and not conflicting with ongoing operations like COPY commands.

## Parameters / Member Variables
- : PostgreSQL connection handle to validate and prepare for query sending
- : Boolean flag indicating whether this represents the start of a new query cycle (affects error state management)

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState: Clears the connection's error state buffer
  - pqClearAsyncResult: Initializes asynchronous result accumulation state
  - CONNECTION_OK: Connection status constant indicating a healthy connection
  - Various PGASYNC_* constants: Asynchronous status values for connection state validation
  - PQ_PIPELINE_OFF: Pipeline status constant indicating normal (non-pipeline) mode
- Called from (representative examples):
  - PQsendQueryInternal: Simple query protocol implementation
  - PQsendQueryParams: Parameterized query sending function
  - PQsendPrepare: Statement preparation function
  - PQsendQueryPrepared: Prepared statement execution function
  - PQsendTypedCommand: Typed command sending function

## Notes and Other Information
- Serves as the central validation point for all query sending operations in libpq
- Implements complex state machine logic for pipeline mode operations, ensuring commands can be safely queued
- Manages error state clearing strategically - only clears errors for new query cycles when no commands are queued
- Prevents conflicting operations by checking connection and asynchronous status before allowing query sending
- Handles both immediate execution mode (non-pipeline) and queued execution mode (pipeline)
- Essential for maintaining connection state consistency across different query execution patterns
- Initializes result accumulation state for non-pipeline operations to prepare for incoming query results
- Enforces operational constraints such as preventing queries during COPY operations
- Foundation function that ensures all higher-level query operations start from a valid, consistent state