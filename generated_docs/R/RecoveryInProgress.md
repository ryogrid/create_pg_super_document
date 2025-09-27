# RecoveryInProgress

## Location
[src/backend/access/transam/xlog.c:6313-6348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6313-L6348)

## Overview
RecoveryInProgress checks whether the database system is still in recovery mode by examining shared memory state, providing a thread-safe way for any process to determine recovery status.

## Definition

```c
bool
RecoveryInProgress(void)
```
## Detailed Description
RecoveryInProgress is a utility function that allows any process connected to shared memory to determine whether the PostgreSQL system is still in recovery mode. Unlike the InRecovery global variable (which is only valid in the startup process), this function can be safely called from any backend process.

The function implements an optimization to minimize shared memory access:
1. **Local Caching**: Uses a process-local variable  to cache the recovery state
2. **One-Way Transition**: Once recovery is complete, it never re-enters recovery mode, so the function stops checking shared state after seeing it false once
3. **Shared State Check**: When still in recovery, it reads the shared  to get the current status
4. **Volatile Access**: Uses volatile pointer to ensure fresh reads of shared memory

The function is widely used throughout PostgreSQL to conditionally enable/disable functionality based on recovery status. Many operations are restricted or behave differently during recovery (e.g., certain WAL operations, transaction handling, backup operations).

## Parameters / Member Variables
- Returns:  - true if recovery is in progress, false if recovery is complete

## Dependencies
- Functions called/Symbols referenced:
  - [XLogCtlData](../X/XLogCtlData.md) (shared memory control structure)
  - RECOVERY_STATE_DONE (recovery completion state constant)
  - LocalRecoveryInProgress (process-local cache variable)
- Called from (representative examples):
  - [GetNewTransactionId](../G/GetNewTransactionId.md) (transaction ID assignment)
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (checkpoint creation logic)
  - [pg_is_in_recovery](../p/pg_is_in_recovery.md) (SQL function)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md) (background writer process)
  - Many other functions across the codebase that need recovery status

## Notes and Other Information
- Thread-safe and can be called from any process with shared memory access
- Implements efficient local caching to reduce shared memory contention
- Critical for determining operational mode throughout PostgreSQL codebase
- Used extensively in conditional logic to handle recovery vs. normal operation
- The volatile pointer access ensures memory barriers are respected
- No explicit memory barrier needed when returning true since recovery could end immediately after
- Process-local caching means the function may return true briefly after recovery ends, but this is acceptable
- Located in src/backend/access/transam/xlog.c:6313-6348

## Simplified Source

```c
// Simplified version of RecoveryInProgress
bool RecoveryInProgress(void) {
    // Quick return if we've already confirmed recovery is complete
    // (optimization: once recovery ends, it never restarts)
    if (!LocalRecoveryInProgress)
        return false;

    // Check shared memory state to get current recovery status
    volatile XLogCtlData *xlogctl = XLogCtl;
    LocalRecoveryInProgress = (xlogctl->SharedRecoveryState != RECOVERY_STATE_DONE);

    return LocalRecoveryInProgress;
}
```

Key simplifications made:
- Removed detailed comments about volatile pointer usage and memory barriers
- Consolidated the if-else structure into clearer flow
- Focused on the core logic: local caching + shared state checking
- Preserved the essential optimization pattern and thread-safety