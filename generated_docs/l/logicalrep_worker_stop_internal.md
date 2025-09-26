# logicalrep_worker_stop_internal

## Location
src/backend/replication/logical/launcher.c: 540 - 621

## Overview
Internal function that safely terminates a logical replication worker process and waits for it to fully detach from its replication slot.

## Definition


## Detailed Description
This function performs a controlled shutdown of a logical replication worker by sending a signal to terminate the worker process and waiting for it to complete cleanup. The function handles two key scenarios: workers that are still starting up (proc not yet set) and fully running workers. It uses a generation-based mechanism to detect if the worker slot has been reused by a different worker during the shutdown process.

The function implements a two-phase approach:
1. First, it waits for workers that are still starting up to finish initialization
2. Then it sends a termination signal and waits for the worker to exit completely

The function uses WaitLatch with timeouts to avoid indefinite blocking, and includes proper interrupt handling and postmaster death detection.

## Parameters / Member Variables
- : Pointer to the LogicalRepWorker structure representing the worker to be stopped
- : Signal number to send to the worker process for termination (typically SIGTERM)

## Dependencies
- Functions called/Symbols referenced:
  - WaitLatch: Used to wait with timeout during worker startup and shutdown phases
  - LWLockHeldByMeInMode: Asserts proper lock acquisition before proceeding
  - LWLockRelease/LWLockAcquire: Manages LogicalRepWorkerLock during wait periods
  - ResetLatch: Clears latch state after wakeup
  - CHECK_FOR_INTERRUPTS: Handles query cancellation and other interrupts
  - kill: Sends termination signal to the worker process

- Called from:
  - logicalrep_worker_stop: Public interface for stopping subscription workers
  - logicalrep_pa_worker_stop: Interface for stopping parallel apply workers  
  - logicalrep_worker_detach: Part of worker cleanup during detachment

## Notes and Other Information
- Requires LogicalRepWorkerLock to be held in LW_SHARED mode before calling
- Uses worker generation numbers to detect slot reuse by different workers
- Implements proper signal handling and graceful shutdown semantics
- Includes timeout-based waiting to prevent indefinite blocking
- Critical for maintaining consistency during logical replication worker lifecycle management