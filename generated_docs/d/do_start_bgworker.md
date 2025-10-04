# do_start_bgworker

## Location
[src/backend/postmaster/postmaster.c:4246-4304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4246-L4304)

## Overview
Starts a new background worker process, handling resource allocation and fork operations for registered background workers in the PostgreSQL postmaster.

## Definition

```c
static bool
do_start_bgworker(RegisteredBgWorker *rw)
```
## Detailed Description
This function is responsible for actually starting a background worker process after timing conditions have been verified. It performs the complete startup sequence including:

1. Allocating a Backend element through assign_backendlist_entry()
2. Forking the worker process via postmaster_child_launch()
3. Managing the worker's state in postmaster data structures
4. Handling failure cases by marking workers as crashed

The function is heavily based on autovacuum.c implementation and follows PostgreSQL's standard pattern for launching child processes. It ensures proper cleanup on failure and maintains consistent state tracking.

## Parameters / Member Variables
- `*rw`: Pointer to RegisteredBgWorker structure containing worker configuration and state information
## Dependencies
- Functions called/Symbols referenced:
  - [assign_backendlist_entry](../a/assign_backendlist_entry.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [postmaster_child_launch](../p/postmaster_child_launch.md)
  - [ReportBackgroundWorkerPID](../R/ReportBackgroundWorkerPID.md)
  - [ReleasePostmasterChildSlot](../R/ReleasePostmasterChildSlot.md)
  - [dlist_push_head](dlist_push_head.md)
  - [ShmemBackendArrayAdd](../S/ShmemBackendArrayAdd.md) (EXEC_BACKEND only)
- Called from (representative examples):
  - MAX_BGWORKERS_TO_LAUNCH (referenced in postmaster logic)

## Notes and Other Information
- Returns true on successful worker start, false on failure
- Updates RegisteredBgWorker state appropriately in both success and failure cases
- On failure, marks the worker as crashed to prevent immediate retry attempts
- Must be called only after timing conditions have been verified
- [Backend](../B/Backend.md) element allocation must occur before forking to handle resource exhaustion cleanly
- Failure handling treats resource exhaustion as a crash to implement backoff behavior

## Simplified Source

```c
static bool do_start_bgworker(RegisteredBgWorker *rw) {
    pid_t worker_pid;

    Assert(rw->rw_pid == 0);

    // Allocate Backend element before forking to handle failures cleanly
    if (!assign_backendlist_entry(rw)) {
        rw->rw_crashed_at = GetCurrentTimestamp();
        return false;
    }

    ereport(DEBUG1, (errmsg_internal("starting background worker process \"%s\"",
                                   rw->rw_worker.bgw_name)));

    // Fork the background worker process
    worker_pid = postmaster_child_launch(B_BG_WORKER, (char *) &rw->rw_worker,
                                        sizeof(BackgroundWorker), NULL);
    if (worker_pid == -1) {
        // Fork failed - cleanup and mark as crashed
        ereport(LOG, (errmsg("could not fork background worker process: %m")));
        ReleasePostmasterChildSlot(rw->rw_child_slot);
        rw->rw_child_slot = 0;
        pfree(rw->rw_backend);
        rw->rw_backend = NULL;
        rw->rw_crashed_at = GetCurrentTimestamp();
        return false;
    }

    // Success - update worker state and add to backend lists
    rw->rw_pid = worker_pid;
    rw->rw_backend->pid = rw->rw_pid;
    ReportBackgroundWorkerPID(rw);
    dlist_push_head(&BackendList, &rw->rw_backend->elem);
#ifdef EXEC_BACKEND
    ShmemBackendArrayAdd(rw->rw_backend);
#endif
    return true;
}
```