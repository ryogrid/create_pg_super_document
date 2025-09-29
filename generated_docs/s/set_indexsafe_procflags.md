# set_indexsafe_procflags

## Location
[src/backend/commands/indexcmds.c:4474-4487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L4474-L4487)

## Overview
set_indexsafe_procflags sets the PROC_IN_SAFE_IC flag in the current process status to optimize concurrent index operations and prevent deadlocks.

## Definition
```c
static inline void set_indexsafe_procflags(void)
```

## Detailed Description
This function sets the PROC_IN_SAFE_IC flag in the current process's status flags to indicate that the process is performing a "safe" index creation that doesn't require other concurrent CREATE INDEX CONCURRENTLY or REINDEX CONCURRENTLY operations to wait for it during their snapshot acquisition phases.

The function serves two main purposes:
1. **Performance optimization**: Prevents other concurrent index operations from unnecessarily waiting for processes that don't conflict with their operations
2. **Deadlock prevention**: Avoids potential deadlock scenarios that can occur when multiple concurrent index operations wait for each other

The "safe" designation applies only to indexes that:
- Are not expressional (don't contain expressions that could access other tables)
- Are not partial (don't have WHERE clauses that could access other tables)

The function acquires the ProcArrayLock in exclusive mode to safely update both the local process status flags and the shared global process status array, ensuring that concurrent processes see a consistent view of the process state.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - MyProc (global process structure)
  - ProcGlobal (global process array)
  - PROC_IN_SAFE_IC (status flag constant)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (multiple locations)

## Notes and Other Information
- The function is static inline, optimized for performance as it's called frequently during index creation
- Includes assertions to ensure it's called before transaction IDs are assigned to prevent XID inconsistencies
- The flag is automatically reset at transaction end, so it must be called for each transaction that needs it
- Must be called before installing xid or xmin in MyProc to avoid backward-moving Xmin values
- Uses exclusive locking on ProcArrayLock to ensure atomic updates to both local and shared process status
- Only safe to use with simple indexes that don't execute expressions accessing other relations
- Caller is responsible for ensuring the index meets the safety criteria (non-expressional and non-partial)
- Part of PostgreSQL's concurrent index building infrastructure designed to improve performance and reliability

## Simplified Source

```c
static inline void set_indexsafe_procflags(void)
{
    // Ensure no transaction IDs are set yet (safety check)
    Assert(MyProc->xid == InvalidTransactionId &&
           MyProc->xmin == InvalidTransactionId);

    // Acquire exclusive lock on process array
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    // Set the "safe index creation" flag
    MyProc->statusFlags |= PROC_IN_SAFE_IC;

    // Update shared process status array
    ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;

    // Release the lock
    LWLockRelease(ProcArrayLock);
}
```