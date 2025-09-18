# _SPI_pquery

## Location
[src/backend/executor/spi.c:2874-2960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2874-L2960)

## Overview
 is an internal SPI (Server Programming Interface) function that executes a prepared query and returns the appropriate result code based on the operation type.

## Definition


## Detailed Description
This function is responsible for executing a query described by a QueryDesc structure within the SPI framework. It determines the appropriate return code based on the SQL command type (SELECT, INSERT, UPDATE, DELETE, MERGE) and whether the statement has a RETURNING clause. The function manages the complete execution lifecycle including starting the executor, running the query, and cleaning up resources.

The function handles different SQL operations by mapping them to specific SPI result codes, with special handling for RETURNING clauses that modify the result code. It also provides optional statistics collection and trigger firing control.

## Parameters / Member Variables
- : Pointer to QueryDesc structure containing the prepared query and execution context
- : Boolean flag indicating whether triggers should be fired during execution
- : Maximum number of tuples to process (0 means no limit)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecutorStart](../E/ExecutorStart.md): Initializes query execution
  - [ExecutorRun](../E/ExecutorRun.md): Executes the query with specified direction and tuple count
  - [ExecutorFinish](../E/ExecutorFinish.md): Completes query execution
  - [ExecutorEnd](../E/ExecutorEnd.md): Cleans up executor resources
  - _SPI_checktuples: Validates SPI tuple count consistency
  - ResetUsage/ShowUsage: Optional statistics collection functions
- Called from (representative examples):
  - [_SPI_execute_plan](_SPI_execute_plan.md): Main SPI execution function

## Notes and Other Information
- Returns different SPI_OK_* codes based on command type and RETURNING clause presence
- Handles special case where SELECT with DestNone destination returns SPI_OK_UTILITY
- Supports optional executor statistics via SPI_EXECUTOR_STATS compilation flag
- Uses EXEC_FLAG_SKIP_TRIGGERS flag when fire_triggers is false
- Updates _SPI_current->processed with the number of tuples processed
- Performs consistency checks on tuple counts for SELECT and RETURNING queries