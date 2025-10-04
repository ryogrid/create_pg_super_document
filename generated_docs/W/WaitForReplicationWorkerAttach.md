# WaitForReplicationWorkerAttach

## Location
[src/backend/replication/logical/launcher.c:183-255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L183-L255)

## Overview
Waits for a background logical replication worker to start up and successfully attach to shared memory, providing cleanup capabilities if the worker fails to attach properly.

## Definition

```c
static bool
WaitForReplicationWorkerAttach(LogicalRepWorker *worker,
							   uint16 generation,
							   BackgroundWorkerHandle *handle)
```
## Detailed Description
WaitForReplicationWorkerAttach implements a polling mechanism to monitor the startup process of a newly launched logical replication worker. The function continuously checks the worker's status in shared memory and monitors the background worker's process state to determine if the worker has successfully attached or has died during startup.

The function uses a combination of shared memory inspection and background worker handle monitoring. It polls the worker's in_use flag and proc field in shared memory to detect successful attachment, while also checking the background worker's process status to detect premature death. If the worker dies before attaching, the function performs cleanup by calling logicalrep_worker_cleanup.

To avoid indefinite blocking, the function uses WaitLatch with a timeout, checking every 10ms for status changes. It carefully manages latch state to avoid interfering with the caller's event handling.

## Parameters / Member Variables
- : Pointer to the LogicalRepWorker structure in shared memory representing the worker being monitored
- : Generation counter to ensure we're monitoring the correct worker instance (prevents race conditions)
- : BackgroundWorkerHandle for querying the worker's process status
- Returns:  - true if worker successfully attached, false if it died or failed to attach

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)  
  - [GetBackgroundWorkerPid](../G/GetBackgroundWorkerPid.md)
  - [logicalrep_worker_cleanup](../l/logicalrep_worker_cleanup.md)
  - [WaitLatch](WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
  - [SetLatch](../S/SetLatch.md)
- Called from:
  - [logicalrep_worker_launch](../l/logicalrep_worker_launch.md)

## Notes and Other Information
- Uses a 10ms polling timeout since worker attachment typically doesn't trigger latch notifications
- Implements careful latch state management to preserve caller's latch events
- Performs generation-based validation to ensure cleanup operations target the correct worker instance
- Essential for preventing shared memory leaks when workers fail during startup
- The function is critical for the launcher's ability to detect and clean up failed worker launches promptly

## Simplified Source

```c
static bool WaitForReplicationWorkerAttach(LogicalRepWorker *worker,
                                           uint16 generation,
                                           BackgroundWorkerHandle *handle)
{
    bool result = false;
    bool dropped_latch = false;

    for (;;) {
        BgwHandleStatus status;
        pid_t pid;
        int rc;

        CHECK_FOR_INTERRUPTS();

        // Check if worker has attached or died
        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
        if (!worker->in_use || worker->proc) {
            result = worker->in_use;  // true if attached, false if died
            LWLockRelease(LogicalRepWorkerLock);
            break;
        }
        LWLockRelease(LogicalRepWorkerLock);

        // Check if background worker process has died
        status = GetBackgroundWorkerPid(handle, &pid);
        if (status == BGWH_STOPPED) {
            // Clean up failed worker
            LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);
            if (generation == worker->generation)
                logicalrep_worker_cleanup(worker);
            LWLockRelease(LogicalRepWorkerLock);
            break;  // result remains false
        }

        // Wait with 10ms timeout for worker attach
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                       10L, WAIT_EVENT_BGWORKER_STARTUP);

        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
            dropped_latch = true;
        }
    }

    // Restore latch if we cleared it during waiting
    if (dropped_latch)
        SetLatch(MyLatch);

    return result;
}
```