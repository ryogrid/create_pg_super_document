# CommandCounterIncrement

## Location
[src/backend/access/transam/xact.c:1097-1148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1097-L1148)

## Overview
CommandCounterIncrement advances the command counter within the current transaction to ensure proper tuple visibility and catalog cache consistency across multiple commands.

## Definition
```c
void CommandCounterIncrement(void)
```

## Detailed Description
This function manages the command counter (`currentCommandId`) within a transaction to maintain proper multi-version concurrency control (MVCC) semantics. The command counter is used to distinguish between different commands within the same transaction, ensuring that each command sees the appropriate snapshot of data based on when it was executed.

The function implements several optimizations and safety checks:

1. **Lazy Increment**: Only increments the counter if `currentCommandIdUsed` is true, meaning the current command ID has actually been used to mark tuples. This optimization helps prevent unnecessary increments for read-only commands and postpones command counter overflow.

2. **Parallel Mode Restrictions**: Prohibits command counter increments during parallel operations (both in parallel mode and for parallel workers) since worker processes synchronize transaction state at the beginning of parallel operations and cannot account for new commands afterward.

3. **Overflow Protection**: Prevents command counter overflow by checking if the new value would equal `InvalidCommandId` (2^32-1), throwing an error if the transaction would exceed the maximum allowed commands.

4. **Snapshot Propagation**: Calls `SnapshotSetCommandId` to propagate the new command ID to static snapshots, ensuring consistent visibility semantics.

5. **Cache Invalidation**: Invokes `AtCCI_LocalCache` to make catalog changes from the just-completed command visible in the local system cache.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsInParallelMode (checks if in parallel mode)
  - IsParallelWorker (checks if current process is a parallel worker)
  - InvalidCommandId (constant representing invalid command ID)
  - SnapshotSetCommandId (propagates command ID to snapshots)
  - AtCCI_LocalCache (invalidates local catalog cache)
- Called from (representative examples):
  - hashadjustmembers, btadjustmembers
  - CommitTransactionCommandInternal, CommitSubTransaction
  - Various DDL operations (index_create, DefineRelation, etc.)
  - Query execution functions (exec_simple_query, PortalRunMulti)
  - Utility command processing functions

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:1093-1138
- Critical component of PostgreSQL's MVCC implementation
- The lazy increment optimization reduces overhead for read-only operations
- Command counter overflow protection ensures transaction stability
- Extensive usage throughout the codebase indicates its fundamental importance to transaction processing
- The parallel mode restrictions ensure consistency in parallel query execution environments