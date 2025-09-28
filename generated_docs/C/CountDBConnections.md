# CountDBConnections

## Location
[src/backend/storage/ipc/procarray.c:3627-3657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3627-L3657)

## Overview
Counts regular database backends (excluding background workers) that are connected to a specified database, providing an accurate count of user connections for administrative and connection limit enforcement purposes.

## Definition
```c
int CountDBConnections(Oid databaseid)
```

## Detailed Description
CountDBConnections is similar to CountDBBackends but with an additional filter to exclude background workers, making it more suitable for counting actual user connections. This distinction is important for enforcing connection limits and understanding the true load from user sessions.

The function iterates through the process array under ProcArrayLock protection, filtering out:
1. Prepared transactions (pid == 0)
2. Background workers (isBackgroundWorker == true)

This provides a count that reflects only regular user backend processes connected to the specified database. Like CountDBBackends, it can count connections to all databases if an invalid OID is provided.

## Parameters / Member Variables
- `databaseid`: The OID of the database for which to count connections. If InvalidOid is passed, counts connections to all databases.

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md) (procArray global variable)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for ProcArrayLock in LW_SHARED mode)
  - [PGPROC](../P/PGPROC.md) (process structure)
  - OidIsValid (macro to check if OID is valid)

- Called from (representative examples):
  - [CheckMyDatabase](CheckMyDatabase.md) (in src/backend/utils/init/postinit.c:386)

## Notes and Other Information
- The key difference from CountDBBackends is the exclusion of background workers, making this more suitable for connection limit checks
- Background workers are system processes that shouldn't count toward user connection limits
- Properly acquires ProcArrayLock for consistent results
- Used during database initialization to check connection limits
- The filtering logic ensures only genuine user connections are counted
- Essential for enforcing per-database connection limits and monitoring actual user load

## Simplified Source

```c
// Simplified version of CountDBConnections
int CountDBConnections(Oid databaseid) {
    ProcArrayStruct *arrayP = procArray;
    int count = 0;
    int index;

    // Acquire shared lock on process array for safe iteration
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    // Iterate through all active processes
    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        PGPROC *proc = &allProcs[pgprocno];

        // Skip prepared transactions (no active process ID)
        if (proc->pid == 0)
            continue;

        // Skip background workers (system processes, not user connections)
        if (proc->isBackgroundWorker)
            continue;

        // Count if targeting all databases or this specific database
        if (!OidIsValid(databaseid) || proc->databaseId == databaseid)
            count++;
    }

    // Release the lock
    LWLockRelease(ProcArrayLock);

    return count;
}
```

Key simplifications made:
- Added descriptive comments explaining each filtering step
- Clarified the purpose of lock acquisition and release
- Explained why prepared transactions and background workers are excluded
- Simplified the database matching logic with clearer comments
- Maintained the exact algorithm structure while improving readability