# test_pipelined_insert

## Location
src/test/modules/libpq_pipeline/libpq_pipeline.c: 1008 - 1253

## Overview
Tests PostgreSQL pipeline mode with bulk insert operations using prepared statements in non-blocking mode, demonstrating efficient handling of large-scale data insertion within a transaction pipeline.

## Definition


## Detailed Description
This function performs a comprehensive test of PostgreSQL's pipeline mode for bulk insert operations. It creates a complete transaction workflow including table creation, prepared statement setup, and bulk data insertion using non-blocking I/O to avoid deadlocks. The test follows a state machine approach with distinct phases:

1. **Setup Phase**: Begins transaction, drops/creates test table, and prepares an INSERT statement
2. **Insert Phase**: Executes bulk inserts using the prepared statement with parameterized values
3. **Cleanup Phase**: Commits the transaction and synchronizes the pipeline

The function uses non-blocking mode with select() to interleave sending commands and receiving results, preventing buffer overflow scenarios that could cause deadlocks during high-volume operations. Each phase is tracked through a PipelineInsertStep state machine that coordinates both sending and receiving operations.

## Parameters / Member Variables
- : PostgreSQL connection handle for pipeline operations
- : Number of rows to insert during the bulk insert test

## Dependencies
- Functions called/Symbols referenced:
  - PQenterPipelineMode/PQexitPipelineMode (pipeline mode control)
  - [PQsendQueryParams](../P/PQsendQueryParams.md) (sending SQL commands)
  - [PQsendPrepare](../P/PQsendPrepare.md) (preparing statements)
  - [PQsendQueryPrepared](../P/PQsendQueryPrepared.md) (executing prepared statements)
  - [PQsetnonblocking](../P/PQsetnonblocking.md) (enabling non-blocking I/O)
  - [PQsocket](../P/PQsocket.md)/select/FD_SET/FD_ZERO (socket-level I/O management)
  - [PQconsumeInput](../P/PQconsumeInput.md)/PQisBusy/PQgetResult (result processing)
  - PQpipelineSync (pipeline synchronization)
  - [PQflush](../P/PQflush.md) (forcing output buffer flush)
  - PipelineInsertStep enum and BI_* constants (state machine states)
  - MAXINTLEN/MAXINT8LEN (parameter formatting constants)
- Called from (representative examples):
  - [main](../m/main.md) (at src/test/modules/libpq_pipeline/libpq_pipeline.c:2266)

## Notes and Other Information
- Demonstrates proper non-blocking pipeline handling to prevent deadlocks during bulk operations
- Uses prepared statements for efficient parameter binding during bulk inserts
- Implements a state machine pattern for coordinating complex multi-phase pipeline operations
- Tests wide integer values (1LL << 62) to exercise buffer space management
- Validates proper command tag verification for each pipeline phase
- Essential test for verifying pipeline mode scalability with large data volumes
- Part of the libpq_pipeline test suite for PostgreSQL client library validation