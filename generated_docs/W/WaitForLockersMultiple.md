# WaitForLockersMultiple

## Location
src/backend/storage/lmgr/lmgr.c: 903 - 980

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
  - GetLockConflicts
  - VirtualTransactionIdIsValid  
  - VirtualXactLock
  - ProcNumberGetProc
  - pgstat_progress_update_param
  - pgstat_progress_update_multi_param
  - list_free_deep
- Called from (representative examples):
  - ATExecDetachPartition (in tablecmds.c:19266)
  - WaitForLockers (in lmgr.c:986)

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