# StartAutovacuumWorker

## Location
[src/backend/postmaster/postmaster.c:3962-4052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3962-L4052)

## Overview
StartAutovacuumWorker creates and manages autovacuum worker processes, handling the full lifecycle from process creation to backend registration and error handling.

## Definition

```c
static void
StartAutovacuumWorker(void)
```
## Detailed Description
StartAutovacuumWorker is responsible for creating autovacuum worker processes in PostgreSQL. Unlike simple auxiliary processes, autovacuum workers require more complex setup including backend registration, cancel key generation, and slot assignment. The function first checks if the system is in a suitable state to accept autovacuum connections using canAcceptConnections(). 

If conditions are favorable, it generates a random cancel key for the worker process, allocates a Backend structure, and assigns a postmaster child slot. The actual process creation is delegated to StartChildProcess() with the B_AUTOVAC_WORKER type. Upon successful creation, the worker is registered in the BackendList and in shared memory (on EXEC_BACKEND builds).

The function includes comprehensive error handling - if process creation fails, it cleans up allocated resources and notifies the autovacuum launcher about the failure. This notification mechanism helps prevent rapid retry loops between the launcher and postmaster.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [canAcceptConnections](../c/canAcceptConnections.md) (checks if system can accept new connections)
  - [RandomCancelKey](../R/RandomCancelKey.md) (generates random cancel key for security)
  - [palloc_extended](../p/palloc_extended.md) (memory allocation with no-OOM option)
  - [AssignPostmasterChildSlot](../A/AssignPostmasterChildSlot.md) (assigns child process slot)
  - [StartChildProcess](StartChildProcess.md) (creates the actual worker process)
  - [dlist_push_head](../d/dlist_push_head.md) (adds to backend list)
  - [ShmemBackendArrayAdd](ShmemBackendArrayAdd.md) (shared memory registration on EXEC_BACKEND)
  - [ReleasePostmasterChildSlot](../R/ReleasePostmasterChildSlot.md) (cleanup on failure)
  - [AutoVacWorkerFailed](../A/AutoVacWorkerFailed.md) (notifies launcher of failure)
- Called from (representative examples):
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md) (signal handler for autovac worker requests)

## Notes and Other Information
- Creates Backend structures for proper process tracking and management
- Generates cancel keys for security, even though autovac workers may not need them
- Implements careful resource cleanup on failure to prevent memory leaks
- Includes race condition prevention by checking system state before proceeding
- Uses deferred signaling approach to avoid ping-pong effects with the autovac launcher
- Code structure roughly matches BackendStartup for consistency
- Workers are marked as non-dead_end processes requiring child slots

## Simplified Source

```c
// Simplified version of StartAutovacuumWorker
static void StartAutovacuumWorker(void) {
    Backend *bn;

    // Check if system can accept autovac connections
    if (canAcceptConnections(BACKEND_TYPE_AUTOVAC) == CAC_OK) {

        // Generate security cancel key
        if (!RandomCancelKey(&MyCancelKey)) {
            ereport(LOG, (errmsg("could not generate random cancel key")));
            return;
        }

        // Allocate backend structure
        bn = (Backend *) palloc_extended(sizeof(Backend), MCXT_ALLOC_NO_OOM);
        if (bn) {
            // Initialize backend properties
            bn->cancel_key = MyCancelKey;
            bn->dead_end = false;
            bn->child_slot = MyPMChildSlot = AssignPostmasterChildSlot();
            bn->bgworker_notify = false;

            // Create the worker process
            bn->pid = StartChildProcess(B_AUTOVAC_WORKER);
            if (bn->pid > 0) {
                // Success: register the worker
                bn->bkend_type = BACKEND_TYPE_AUTOVAC;
                dlist_push_head(&BackendList, &bn->elem);
                #ifdef EXEC_BACKEND
                ShmemBackendArrayAdd(bn);
                #endif
                return;
            }

            // Process creation failed - cleanup
            ReleasePostmasterChildSlot(bn->child_slot);
            pfree(bn);
        } else {
            ereport(LOG, (errmsg("out of memory")));
        }
    }

    // Notify launcher of failure (if it's running)
    if (AutoVacPID != 0) {
        AutoVacWorkerFailed();
        avlauncher_needs_signal = true;
    }
}
```

Key simplifications made:
- Consolidated error handling paths for clarity
- Removed detailed comments that explained implementation details
- Simplified conditional structure while preserving all logic paths
- Focused on the main execution flow: check conditions → allocate → create → register → cleanup on failure
- Preserved all essential functionality including security, resource management, and error reporting