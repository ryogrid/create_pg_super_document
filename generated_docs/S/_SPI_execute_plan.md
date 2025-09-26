# _SPI_execute_plan

## Location
[src/backend/executor/spi.c:2399-2848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2399-L2848)

## Overview
_SPI_execute_plan is the core internal function that executes prepared SQL plans with comprehensive options for controlling execution behavior, snapshot management, and result handling.

## Definition
```c
static int _SPI_execute_plan(SPIPlanPtr plan, const SPIExecuteOptions *options,
                            Snapshot snapshot, Snapshot crosscheck_snapshot,
                            bool fire_triggers)
```

## Detailed Description
The _SPI_execute_plan function is the central execution engine for the Server Programming Interface (SPI). It handles the complete execution lifecycle of prepared SQL plans, including snapshot management, parameter binding, command execution, and result collection.

The function supports four distinct snapshot management behaviors based on the provided snapshot parameter and read-only mode. It handles both regular prepared plans and one-shot plans, performing deferred parse analysis for the latter. The function manages atomic and non-atomic execution contexts, enforces read-only restrictions, and processes both utility statements and planned queries.

For each statement in the plan, it sets up appropriate destination receivers, manages command counter increments, handles transaction semantics, and collects execution results. The function provides comprehensive error handling and resource cleanup.

## Parameters / Member Variables
- `plan`: SPIPlanPtr containing the prepared plan to execute
- `options`: SPIExecuteOptions structure containing execution parameters including params, read_only flag, tuple count limit, and destination receiver
- `snapshot`: Query snapshot to use, or InvalidSnapshot for normal snapshot behavior  
- `crosscheck_snapshot`: Snapshot for referential integrity checks, typically InvalidSnapshot
- `fire_triggers`: Whether to fire AFTER triggers at query end (true) or postpone to outer query (false)

## Dependencies
- Functions called/Symbols referenced:
  - [IsSubTransaction](../I/IsSubTransaction.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)/PopActiveSnapshot
  - [GetCachedPlan](../G/GetCachedPlan.md)/ReleaseCachedPlan
  - [CreateQueryDesc](../C/CreateQueryDesc.md)/FreeQueryDesc
  - [ProcessUtility](../P/ProcessUtility.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [_SPI_pquery](_SPI_pquery.md)
  - [pg_analyze_and_rewrite_withcb](../p/pg_analyze_and_rewrite_withcb.md)
  - [pg_analyze_and_rewrite_fixedparams](../p/pg_analyze_and_rewrite_fixedparams.md)
  - [CompleteCachedPlan](../C/CompleteCachedPlan.md)
- Called from (representative examples):
  - [SPI_execute](SPI_execute.md)
  - [SPI_execute_extended](SPI_execute_extended.md)
  - [SPI_execute_plan](SPI_execute_plan.md)
  - [SPI_execute_plan_extended](SPI_execute_plan_extended.md)
  - [SPI_execute_with_args](SPI_execute_with_args.md)

## Notes and Other Information
- Returns SPI result codes (SPI_OK_*, SPI_ERROR_*)
- Sets global SPI_processed and SPI_tuptable variables for caller access
- Supports both atomic and non-atomic execution contexts based on connection options
- Handles one-shot plans by performing deferred parse analysis during execution
- Manages complex snapshot semantics for different read/write and atomic/non-atomic combinations
- Validates that must_return_tuples queries actually return tuples
- Prevents execution of unsupported statements like COPY without filename or transaction statements
- Updates command counter between statements in write mode for visibility
- Transfers tuple table ownership from SPI context to caller