# WaitForLockersMultiple

## Location
[src/backend/storage/lmgr/lmgr.c:903-980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L903-L980)

## Overview
Waits for all transactions that hold conflicting locks on multiple lock tags to complete before proceeding.

## Definition
```c
void WaitForLockersMultiple(List *locktags, LOCKMODE lockmode, bool progress)
```

## Detailed Description
This function implements a comprehensive waiting mechanism that allows a transaction to wait for completion of all other transactions that hold locks conflicting with a specified lock mode on a list of lock targets. It's an extension of the single-lock waiting functionality to handle multiple lock tags simultaneously.

The function operates in two phases: first, it collects all transactions that currently hold conflicting locks on any of the specified lock tags using GetLockConflicts. Second, it waits for each of these transactions to complete by acquiring and immediately releasing virtual transaction locks (VXIDs) on each holder.

Importantly, the function does not attempt to acquire locks on the target objects themselves - it only waits for existing lock holders to complete. This means that new transactions that acquire conflicting locks after the initial scan will not be waited for.

The function includes optional progress reporting functionality that can update PostgreSQL's progress reporting system with information about the total number of transactions to wait for, how many have completed, and which transaction is currently being waited on.

## Parameters / Member Variables
- `locktags`: List of LOCKTAG structures representing the objects to check for lock conflicts
- `lockmode`: The lock mode that would conflict with existing locks on the specified objects
- `progress`: Boolean flag indicating whether to report progress information to the statistics system

## Dependencies
- Functions called/Symbols referenced:
  - [GetLockConflicts](../G/GetLockConflicts.md)
  - VirtualTransactionIdIsValid  
  - [VirtualXactLock](../V/VirtualXactLock.md)
  - [ProcNumberGetProc](../P/ProcNumberGetProc.md)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md)
  - [list_free_deep](../l/list_free_deep.md)
- Called from (representative examples):
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md) (in tablecmds.c:19266)
  - [WaitForLockers](WaitForLockers.md) (in lmgr.c:986)

## Notes and Other Information
- Returns immediately if the locktags list is empty (NIL)
- Never reports or waits for the calling transaction's own locks
- Includes support for prepared transactions in the waiting logic
- Progress reporting uses PROGRESS_WAITFOR_* constants for different metrics:
  - PROGRESS_WAITFOR_TOTAL: Total transactions to wait for
  - PROGRESS_WAITFOR_DONE: Number of transactions completed
  - PROGRESS_WAITFOR_CURRENT_PID: Process ID of currently waited transaction
- Resets all progress counters to zero when complete
- Memory management: properly frees the list of lock holders using list_free_deep
- Commonly used in DDL operations that need to ensure exclusive access to database objects
- Essential for operations like partition detachment where conflicts with ongoing transactions must be resolved

## Simplified Source

```c
void WaitForLockersMultiple(List *locktags, LOCKMODE lockmode, bool progress) {
    List *holders = NIL;
    int total = 0;
    int done = 0;

    // Nothing to do if no lock tags provided
    if (locktags == NIL)
        return;

    // Collect all transactions holding conflicting locks
    foreach(lc, locktags) {
        LOCKTAG *locktag = lfirst(lc);
        int count;

        holders = lappend(holders,
                         GetLockConflicts(locktag, lockmode,
                                         progress ? &count : NULL));
        if (progress)
            total += count;
    }

    // Update progress with total count if requested
    if (progress)
        pgstat_progress_update_param(PROGRESS_WAITFOR_TOTAL, total);

    // Wait for each transaction to complete
    foreach(lc, holders) {
        VirtualTransactionId *lockholders = lfirst(lc);

        while (VirtualTransactionIdIsValid(*lockholders)) {
            // Report current PID if progress tracking enabled
            if (progress) {
                PGPROC *holder = ProcNumberGetProc(lockholders->procNumber);
                if (holder)
                    pgstat_progress_update_param(PROGRESS_WAITFOR_CURRENT_PID,
                                                holder->pid);
            }

            // Wait for this transaction to finish
            VirtualXactLock(*lockholders, true);
            lockholders++;

            // Update completion count
            if (progress)
                pgstat_progress_update_param(PROGRESS_WAITFOR_DONE, ++done);
        }
    }

    // Reset progress counters if tracking was enabled
    if (progress) {
        const int64 values[] = {0, 0, 0};
        pgstat_progress_update_multi_param(3,
                                          (int[]){PROGRESS_WAITFOR_TOTAL,
                                                  PROGRESS_WAITFOR_DONE,
                                                  PROGRESS_WAITFOR_CURRENT_PID},
                                          values);
    }

    // Clean up memory
    list_free_deep(holders);
}
```