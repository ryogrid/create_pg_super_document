# EnterParallelMode

## Location
[src/backend/access/transam/xact.c:1048-1060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1048-L1060)

## Overview
EnterParallelMode increments the parallel mode nesting level for the current transaction, enabling parallel execution capabilities.

## Definition
```c
void EnterParallelMode(void)
```

## Detailed Description
This function manages the parallel mode state within a PostgreSQL transaction by incrementing the `parallelModeLevel` counter in the current transaction state. PostgreSQL uses a nesting model for parallel mode, where multiple operations can enter parallel mode independently, and the system remains in parallel mode until all operations have exited.

The function performs a simple increment operation on the parallel mode level counter, which tracks how deeply nested the current transaction is in parallel execution contexts. This counter-based approach allows for proper nesting of parallel operations and ensures that parallel mode is only fully exited when all parallel contexts have been properly unwound.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type definition)
  - CurrentTransactionState (global transaction state variable)
- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)  
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)
  - [CommitTransaction](../C/CommitTransaction.md)
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md)
  - [ExecutePlan](ExecutePlan.md)

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:1044-1055
- Uses Assert to ensure parallelModeLevel is non-negative before incrementing
- Part of PostgreSQL's parallel query execution infrastructure
- Must be paired with corresponding ExitParallelMode calls to maintain proper nesting
- The nesting level approach allows multiple subsystems to independently manage their parallel execution state

## Simplified Source

```c
// Simplified version of EnterParallelMode
void EnterParallelMode(void) {
    TransactionState s = CurrentTransactionState;

    // Ensure parallel mode level is valid
    Assert(s->parallelModeLevel >= 0);

    // Increment parallel mode nesting level
    ++s->parallelModeLevel;
}
```

Key simplifications made:
- Preserved the essential parallel mode level increment
- Kept the safety assertion for level validation
- Maintained the straightforward nesting counter logic
- Focused on the core parallel mode tracking mechanism