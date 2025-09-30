# TerminateOtherDBBackends

## Location
[src/backend/storage/ipc/procarray.c:3827-3941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3827-L3941)

## Overview
Terminates existing connections to a specified database, typically used by the DROP DATABASE command when the user has requested to forcefully drop the database.

## Definition
```c
void TerminateOtherDBBackends(Oid databaseId)
```

## Detailed Description
This function forcefully terminates all backend processes connected to the specified database, excluding the current backend process. It is primarily used by the DROP DATABASE command with the FORCE option. The function performs comprehensive permission checks before terminating any processes and fails completely if prepared transactions exist for the target database or if permission checks fail for any connection.

The function operates in several phases:
1. Scans the process array to identify backends connected to the target database
2. Collects process IDs while checking for prepared transactions
3. Validates permissions for terminating each identified process
4. Sends SIGTERM signals to terminate the processes

## Parameters / Member Variables
- `databaseId`: The OID of the database whose connections should be terminated

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for process array synchronization)
  - [lappend_int](../l/lappend_int.md) (for building list of process IDs)
  - [get_database_name](../g/get_database_name.md) (for error reporting)
  - [BackendPidGetProc](../B/BackendPidGetProc.md) (for process lookup)
  - [superuser](../s/superuser.md)/superuser_arg (for permission checks)
  - [has_privs_of_role](../h/has_privs_of_role.md) (for role privilege validation)
  - kill (for sending termination signals)
- Called from (representative examples):
  - [dropdb](../d/dropdb.md) (in dbcommands.c)

## Notes and Other Information
- The current backend is always ignored; caller must check if current backend uses the target database
- Fails immediately if any prepared transactions exist for the target database
- Permission checks are more relaxed than pg_terminate_backend, allowing termination of autovacuum workers and background workers
- Uses SIGTERM signal, with process group signaling on systems with setsid()
- Race condition exists between process identification and termination, but is considered acceptable
- Function is atomic in permission checking - if any process cannot be terminated due to permissions, the entire operation fails

## Simplified Source

```c
void TerminateOtherDBBackends(Oid databaseId) {
    ProcArrayStruct *arrayP = procArray;
    List *pids = NIL;
    int nprepared = 0;
    int i;

    // Collect processes connected to target database
    LWLockAcquire(ProcArrayLock, LW_SHARED);
    for (i = 0; i < procArray->numProcs; i++) {
        int pgprocno = arrayP->pgprocnos[i];
        PGPROC *proc = &allProcs[pgprocno];

        // Skip processes not using this database or current process
        if (proc->databaseId != databaseId || proc == MyProc)
            continue;

        // Collect PIDs, count prepared transactions
        if (proc->pid != 0)
            pids = lappend_int(pids, proc->pid);
        else
            nprepared++;
    }
    LWLockRelease(ProcArrayLock);

    // Fail if prepared transactions exist
    if (nprepared > 0) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                       errmsg("database \"%s\" is being used by prepared transactions",
                              get_database_name(databaseId))));
    }

    // Check permissions for all processes before terminating any
    if (pids) {
        ListCell *lc;
        foreach(lc, pids) {
            int pid = lfirst_int(lc);
            PGPROC *proc = BackendPidGetProc(pid);

            if (proc != NULL) {
                // Check superuser permission
                if (superuser_arg(proc->roleId) && !superuser()) {
                    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                   errmsg("permission denied to terminate process")));
                }

                // Check role privilege permission
                if (!has_privs_of_role(GetUserId(), proc->roleId) &&
                    !has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_BACKEND)) {
                    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                   errmsg("permission denied to terminate process")));
                }
            }
        }

        // Terminate all processes
        foreach(lc, pids) {
            int pid = lfirst_int(lc);
            PGPROC *proc = BackendPidGetProc(pid);

            if (proc != NULL) {
#ifdef HAVE_SETSID
                (void) kill(-pid, SIGTERM);  // Signal process group
#else
                (void) kill(pid, SIGTERM);   // Signal individual process
#endif
            }
        }
    }
}
```