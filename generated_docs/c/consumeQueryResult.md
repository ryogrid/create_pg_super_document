# consumeQueryResult

## Location
src/fe_utils/parallel_slot.c: 58 - 79

## Overview
A static function that consumes and processes all pending query results from a parallel slot connection until none remain, handling cancellation setup and tracking overall success.

## Definition
```c
static bool consumeQueryResult(ParallelSlot *slot)
```

## Detailed Description
This function is responsible for completely draining all query results from a given parallel slot's database connection. It sets up proper cancellation handling, then enters a loop to retrieve and process each result using `PQgetResult`. Each individual result is processed through the `processQueryResult` function, and the overall operation is considered successful only if all individual results are processed successfully. The function blocks until all results are consumed, making it suitable for synchronous result processing scenarios.

## Parameters / Member Variables
- `slot`: A ParallelSlot pointer containing the database connection and result processing context

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelSlot](../P/ParallelSlot.md) (struct type)
  - [SetCancelConn](../S/SetCancelConn.md) (sets up cancellation handling for the connection)
  - [PQgetResult](../P/PQgetResult.md) (PostgreSQL libpq function to retrieve query results)
  - [processQueryResult](../p/processQueryResult.md) (processes individual query results)
  - [ResetCancelConn](../R/ResetCancelConn.md) (resets cancellation handling)
- Called from (representative examples):
  - [ParallelSlotsWaitCompletion](../P/ParallelSlotsWaitCompletion.md)

## Notes and Other Information
- This is a static function, only accessible within the parallel_slot.c file
- The function will block until all results are consumed from the connection
- Uses cancellation handling to allow for graceful interruption during result consumption
- Returns false if any individual result processing fails, but continues processing remaining results
- Part of PostgreSQL's frontend utility library for managing parallel query execution
- The function ensures proper cleanup by consuming all results even if some fail to process