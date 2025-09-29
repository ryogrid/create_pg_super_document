# MinimumActiveBackends

## Location
[src/backend/storage/ipc/procarray.c:3545-3597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3545-L3597)

## Overview
Counts backends (other than the caller) that are in active transactions and returns true if the count exceeds a minimum threshold, used as a heuristic to decide if a pre-XLOG-flush delay is worthwhile during commit.

## Definition
```c
bool MinimumActiveBackends(int min)
```

## Detailed Description
MinimumActiveBackends is a performance optimization function that helps decide whether to introduce a delay before flushing the XLOG during transaction commit. The function counts active backends (excluding the caller) that are currently executing transactions and compares this count against a specified minimum threshold.

The function deliberately does not acquire ProcArrayLock for performance reasons, making it slightly racy but acceptable since it's only used for heuristic purposes. It filters out backends that are:
- Blocked waiting for locks (not actively running)
- Without assigned transaction IDs (not in active transactions)
- Prepared transactions (pid == 0)
- Deleted entries (pgprocno == -1)
- The calling process itself

The rationale is that if there are many active backends, introducing a small delay before XLOG flush might allow more transactions to batch together, improving overall throughput.

## Parameters / Member Variables
- `min`: The minimum threshold of active backends required. If 0, the function immediately returns true as a short-circuit optimization.

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md) (procArray global variable)
  - [PGPROC](../P/PGPROC.md) (process structure)
  - MyProc (current process)
  - InvalidTransactionId (constant for invalid transaction ID)

- Called from (representative examples):
  - [XLogFlush](../X/XLogFlush.md) (in src/backend/access/transam/xlog.c:2881)

## Notes and Other Information
- The function intentionally operates without acquiring ProcArrayLock for performance, accepting slight race conditions since the result is only used heuristically
- The function can handle concurrent modifications to the process array gracefully by checking for invalid entries
- This is part of PostgreSQL's commit performance optimization strategy, where batching multiple commits can improve I/O efficiency
- The function assumes that backends blocked on locks are not contributing to system load and excludes them from the count

## Simplified Source

```c
bool MinimumActiveBackends(int min)
{
    ProcArrayStruct *arrayP = procArray;
    int count = 0;
    int index;

    // Quick short-circuit if no minimum is specified
    if (min == 0)
        return true;

    // Note: Don't acquire ProcArrayLock for speed
    // Slightly racy but OK since only used for heuristics
    for (index = 0; index < arrayP->numProcs; index++)
    {
        int pgprocno = arrayP->pgprocnos[index];
        PGPROC *proc = &allProcs[pgprocno];

        // Handle potential garbage due to lack of locking
        if (pgprocno == -1)
            continue;           // Skip deleted entries
        if (proc == MyProc)
            continue;           // Don't count myself
        if (proc->xid == InvalidTransactionId)
            continue;           // Skip if no XID assigned
        if (proc->pid == 0)
            continue;           // Don't count prepared transactions
        if (proc->waitLock != NULL)
            continue;           // Don't count if blocked on lock

        count++;
        if (count >= min)
            break;              // Early exit optimization
    }

    return count >= min;
}
```