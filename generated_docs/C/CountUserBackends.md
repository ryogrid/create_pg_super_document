# CountUserBackends

## Location
[src/backend/storage/ipc/procarray.c:3699-3748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3699-L3748)

## Overview
Counts regular backends (excluding background workers) that are running under a specified user role, used for enforcing per-user connection limits and monitoring user activity.

## Definition
```c
int CountUserBackends(Oid roleid)
```

## Detailed Description
CountUserBackends provides a count of active user connections for a specific role/user, which is essential for enforcing per-user connection limits in PostgreSQL. The function filters the process array to count only genuine user backends, excluding both prepared transactions and background workers.

This function is similar to CountDBConnections but focuses on user-based rather than database-based filtering. It's particularly important for multi-user environments where connection limits need to be enforced on a per-user basis to prevent any single user from monopolizing system resources.

The function iterates through all processes under ProcArrayLock protection, matching against the roleId field in the PGPROC structure to identify backends belonging to the specified user.

## Parameters / Member Variables
- `roleid`: The OID of the role/user for which to count backends. Only backends running under this role will be counted.

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md) (procArray global variable)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for ProcArrayLock in LW_SHARED mode)
  - [PGPROC](../P/PGPROC.md) (process structure)

- Called from (representative examples):
  - [InitializeSessionUserId](../I/InitializeSessionUserId.md) (in src/backend/utils/init/miscinit.c:871)

## Notes and Other Information
- Excludes both prepared transactions and background workers, ensuring only real user connections are counted
- Essential for enforcing per-user connection limits in multi-user environments
- Used during session initialization to check if a user has exceeded their connection limit
- The filtering ensures accurate counts for resource management and user monitoring
- Acquires shared lock for consistent results across the counting operation
- Part of PostgreSQL's role-based access control and resource management system

## Simplified Source

```c
// Simplified version of CountUserBackends
int CountUserBackends(Oid roleid) {
    ProcArrayStruct *arrayP = procArray;
    int count = 0;

    // Lock the process array for consistent reading
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    // Iterate through all processes in the array
    for (int index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        PGPROC *proc = &allProcs[pgprocno];

        // Skip inactive processes (prepared transactions)
        if (proc->pid == 0)
            continue;

        // Skip background workers - only count regular user backends
        if (proc->isBackgroundWorker)
            continue;

        // Count if this process belongs to the target role
        if (proc->roleId == roleid)
            count++;
    }

    LWLockRelease(ProcArrayLock);
    return count;
}
```

Key simplifications made:
- Focused on the core counting algorithm with filtering
- Added clear comments for each filtering condition
- Emphasized the lock acquisition pattern for consistency
- Showed the three-step filter: active, non-background, matching role
- Simplified loop structure while preserving logic