# IsInParallelMode

## Location
[src/backend/access/transam/xact.c:1086-1096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1086-L1096)

## Overview
IsInParallelMode determines whether the current backend is operating in parallel mode, either as a leader or worker process in a parallel operation.

## Definition
```c
bool IsInParallelMode(void)
```

## Detailed Description
This function checks if the current backend is participating in a parallel operation by examining the transaction state's parallel mode indicators. It returns true when either the `parallelModeLevel` is non-zero (indicating active parallel mode) or when `parallelChildXact` is true (indicating we're in a subtransaction of a parallel operation).

The function serves as a critical guard to prohibit operations that change backend-local state expected to match across all parallel workers. This includes operations that modify shared state in ways that could cause inconsistencies between the leader and worker processes. The documentation notes that mere caches usually don't require such restrictions, and state modified in a strict push/pop fashion (like the active snapshot stack) is often acceptable.

The parallel mode detection extends to subtransactions - if we're in a subtransaction of a transaction that initiated a parallel operation, the function still returns true, as those contexts have the same operational restrictions.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type definition)
  - CurrentTransactionState (global transaction state variable)
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md), heap_update, heap_inplace_update
  - [CreateParallelContext](../C/CreateParallelContext.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [PrepareTransaction](../P/PrepareTransaction.md)
  - Various predicate locking functions
  - [PreventCommandIfParallelMode](../P/PreventCommandIfParallelMode.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md), UpdateActiveSnapshotCommandId

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:1072-1091
- Critical for maintaining consistency in parallel query execution
- Used extensively throughout the codebase to prevent unsafe operations during parallel execution
- The function covers both direct parallel mode (`parallelModeLevel != 0`) and inherited parallel context (`parallelChildXact`)
- Essential for PostgreSQL's parallel safety mechanisms across multiple subsystems including heap operations, transaction management, and snapshot handling

## Simplified Source

```c
// Simplified version of IsInParallelMode
bool IsInParallelMode(void) {
    // Get current transaction state
    TransactionState s = CurrentTransactionState;

    // Check if we're in parallel mode or a child transaction of parallel mode
    return s->parallelModeLevel != 0 || s->parallelChildXact;
}
```

Key simplifications made:
- Added clear comments explaining the parallel mode check
- Preserved the essential transaction state examination
- Maintained the dual condition logic for both direct and inherited parallel mode
- Function is already concise and well-focused