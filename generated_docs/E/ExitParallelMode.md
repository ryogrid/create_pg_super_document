# ExitParallelMode

## Location
[src/backend/access/transam/xact.c:1061-1085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1061-L1085)

## Overview
ExitParallelMode decrements the parallel mode nesting level for the current transaction, potentially disabling parallel execution capabilities when the level reaches zero.

## Definition
```c
void ExitParallelMode(void)
```

## Detailed Description
This function manages the exit from parallel mode within a PostgreSQL transaction by decrementing the `parallelModeLevel` counter in the current transaction state. It serves as the counterpart to `EnterParallelMode` and implements proper nesting semantics for parallel execution contexts.

The function includes assertions to ensure proper usage: it verifies that the parallel mode level is greater than 0 (meaning we're actually in parallel mode), and performs additional checks to ensure that parallel contexts are properly managed. Specifically, it asserts that either the parallel mode level will remain above 1 after decrementing, or that we're in a parallel child transaction, or that no parallel context is currently active.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type definition)
  - CurrentTransactionState (global transaction state variable)
  - [ParallelContextActive](../P/ParallelContextActive.md) (function to check if parallel context is active)
- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md)
  - [_brin_end_parallel](../b/_brin_end_parallel.md)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)
  - [_bt_end_parallel](../b/_bt_end_parallel.md)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)
  - [parallel_vacuum_end](../p/parallel_vacuum_end.md)
  - [ExecutePlan](ExecutePlan.md)

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:1057-1070
- Contains comprehensive assertions to prevent improper parallel mode state transitions
- Must be paired with corresponding EnterParallelMode calls to maintain proper nesting
- The assertion checking ParallelContextActive ensures that parallel contexts are properly cleaned up before exiting the last level of parallel mode
- Critical for maintaining the integrity of PostgreSQL's parallel execution framework

## Simplified Source

```c
void
ExitParallelMode(void)
{
    TransactionState s = CurrentTransactionState;

    Assert(s->parallelModeLevel > 0);
    Assert(s->parallelModeLevel > 1 || s->parallelChildXact ||
           !ParallelContextActive());

    // Decrement the parallel mode nesting level
    --s->parallelModeLevel;
}
```