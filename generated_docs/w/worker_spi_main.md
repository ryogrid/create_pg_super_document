# worker_spi_main

## Location
src/test/modules/worker_spi/worker_spi.c: 138 - 305

## Overview
This is the main entry point function for PostgreSQL background worker processes in the worker_spi test module, implementing a continuous loop that performs database operations via SPI.

## Definition
```c
void worker_spi_main(Datum main_arg)
```

## Detailed Description
The `worker_spi_main` function serves as the primary execution function for worker_spi background workers. It:

1. **Initialization Phase**: 
   - Extracts worker index from main_arg parameter
   - Parses database OID, role OID, and flags from bgw_extra data
   - Sets up signal handlers for SIGHUP and SIGTERM
   - Establishes database connection using provided credentials or fallback GUCs

2. **Schema Setup**: 
   - Creates a worktable structure with schema name pattern "schema{N}" where N is the worker index
   - Calls initialize_worker_spi to ensure schema and table exist
   - Quotes identifiers for SQL safety

3. **Main Processing Loop**:
   - Executes a complex SQL query that processes 'delta' records and updates 'total' records
   - Uses PostgreSQL's latch mechanism for efficient sleeping/waiting
   - Handles configuration reloads (SIGHUP) gracefully
   - Manages proper SPI transaction lifecycle for each iteration
   - Reports activity status to PostgreSQL's statistics system

The function implements a worker that consolidates incremental values ('delta' type) into running totals ('total' type) in a dedicated table.

## Parameters / Member Variables
- `main_arg`: Datum containing the worker index (converted to int32) used for generating unique schema names

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md), BackgroundWorkerUnblockSignals
  - [BackgroundWorkerInitializeConnectionByOid](../B/BackgroundWorkerInitializeConnectionByOid.md), BackgroundWorkerInitializeConnection  
  - [initialize_worker_spi](../i/initialize_worker_spi.md), quote_identifier
  - [WaitEventExtensionNew](../W/WaitEventExtensionNew.md), WaitLatch, ResetLatch
  - [SetCurrentStatementStartTimestamp](../S/SetCurrentStatementStartTimestamp.md), StartTransactionCommand
  - SPI_connect, SPI_execute, SPI_finish
  - GetTransactionSnapshot, PushActiveSnapshot, PopActiveSnapshot
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md), pgstat_report_activity, pgstat_report_stat
- Called from (representative examples):
  - Referenced by PostgreSQL background worker framework (no direct callers in codebase)

## Notes and Other Information
- This function runs in an infinite loop until terminated by SIGTERM
- Implements proper PostgreSQL background worker patterns including latch-based waiting
- Uses custom wait events for better monitoring and debugging
- Supports both static configuration via GUCs and dynamic configuration via bgw_extra
- The SQL query uses CTEs (Common Table Expressions) for atomic delta processing
- Handles interrupts and configuration reloads during execution
- Reports detailed logging when processing records
- Location: src/test/modules/worker_spi/worker_spi.c:138-305