# SetCurrentStatementStartTimestamp

## Location
src/backend/access/transam/xact.c: 911 - 925

## Overview
Sets the timestamp marking the start of the current SQL statement, with special handling for parallel worker processes.

## Definition
void SetCurrentStatementStartTimestamp(void)

## Detailed Description
This function establishes the statement start timestamp by capturing the current time and storing it in the global variable stmtStartTimestamp. The timestamp set by this function provides a consistent time reference that remains constant throughout the entire statement execution, ensuring temporal consistency across all operations within a single statement.

The function includes special logic for parallel processing: in regular backend processes, it captures the current timestamp using GetCurrentTimestamp(). However, in parallel worker processes, the timestamp should already be set by SetParallelStartTimestamps() before the worker begins execution, so the function only asserts that a valid timestamp is present.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - stmtStartTimestamp (global variable)
- Called from (representative examples):
  - [autovac_report_workitem](../a/autovac_report_workitem.md)
  - [begin_replication_step](../b/begin_replication_step.md)
  - [PostgresMain](../P/PostgresMain.md) (multiple locations)
  - [InitPostgres](../I/InitPostgres.md)
  - [initialize_worker_spi](../i/initialize_worker_spi.md)
  - [worker_spi_main](../w/worker_spi_main.md)

## Notes and Other Information
- Must be called at the beginning of each SQL statement execution to establish the baseline timestamp
- In parallel workers, relies on SetParallelStartTimestamps() to provide the timestamp beforehand
- The established timestamp can be retrieved throughout statement execution via GetCurrentStatementStartTimestamp()
- Critical for maintaining consistent time references in logging, monitoring, and time-sensitive operations
- Called from various entry points including the main postgres loop, initialization routines, and worker processes
- The assertion in parallel workers helps catch timing bugs in parallel processing setup