# logicalrep_worker_stop_internal

## Location
[src/backend/replication/logical/launcher.c:540-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L540-L621)

## Overview
Internal function that safely terminates a logical replication worker process and waits for it to fully detach from its replication slot.

## Definition

```c
static void
logicalrep_worker_stop_internal(LogicalRepWorker *worker, int signo)
```
## Detailed Description
This function performs a controlled shutdown of a logical replication worker by sending a signal to terminate the worker process and waiting for it to complete cleanup. The function handles two key scenarios: workers that are still starting up (proc not yet set) and fully running workers. It uses a generation-based mechanism to detect if the worker slot has been reused by a different worker during the shutdown process.

The function implements a two-phase approach:
1. First, it waits for workers that are still starting up to finish initialization
2. Then it sends a termination signal and waits for the worker to exit completely

The function uses WaitLatch with timeouts to avoid indefinite blocking, and includes proper interrupt handling and postmaster death detection.

## Parameters / Member Variables
- `*worker`: Pointer to the LogicalRepWorker structure representing the worker to be stopped
- `signo`: Signal number to send to the worker process for termination (typically SIGTERM)
## Dependencies
- Functions called/Symbols referenced:
  - [WaitLatch](../W/WaitLatch.md): Used to wait with timeout during worker startup and shutdown phases
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md): Asserts proper lock acquisition before proceeding
  - [LWLockRelease](../L/LWLockRelease.md)/LWLockAcquire: Manages LogicalRepWorkerLock during wait periods
  - [ResetLatch](../R/ResetLatch.md): Clears latch state after wakeup
  - CHECK_FOR_INTERRUPTS: Handles query cancellation and other interrupts
  - kill: Sends termination signal to the worker process

- Called from:
  - [logicalrep_worker_stop](logicalrep_worker_stop.md): Public interface for stopping subscription workers
  - [logicalrep_pa_worker_stop](logicalrep_pa_worker_stop.md): Interface for stopping parallel apply workers  
  - [logicalrep_worker_detach](logicalrep_worker_detach.md): Part of worker cleanup during detachment

## Notes and Other Information
- Requires LogicalRepWorkerLock to be held in LW_SHARED mode before calling
- Uses worker generation numbers to detect slot reuse by different workers
- Implements proper signal handling and graceful shutdown semantics
- Includes timeout-based waiting to prevent indefinite blocking
- Critical for maintaining consistency during logical replication worker lifecycle management

## Simplified Source

```c
static void logicalrep_worker_stop_internal(LogicalRepWorker *worker, int signo) {
    Assert(LWLockHeldByMeInMode(LogicalRepWorkerLock, LW_SHARED));

    // Remember worker generation to detect slot reuse
    uint16 generation = worker->generation;

    // Phase 1: Wait for worker to finish starting if not yet fully initialized
    while (worker->in_use && !worker->proc) {
        LWLockRelease(LogicalRepWorkerLock);

        // Wait briefly for worker startup
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                          10L, WAIT_EVENT_BGWORKER_STARTUP);

        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }

        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

        // Check if worker exited or slot was reused
        if (!worker->in_use || worker->generation != generation)
            return;

        if (worker->proc)
            break;  // Worker fully started
    }

    // Phase 2: Send termination signal
    kill(worker->proc->pid, signo);

    // Phase 3: Wait for worker to exit
    for (;;) {
        // Check if worker has exited
        if (!worker->proc || worker->generation != generation)
            break;

        LWLockRelease(LogicalRepWorkerLock);

        // Wait briefly for worker shutdown
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                          10L, WAIT_EVENT_BGWORKER_SHUTDOWN);

        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }

        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
    }
}
```