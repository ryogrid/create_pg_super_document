# TerminateBackgroundWorker

## Location
[src/backend/postmaster/bgworker.c:1221-1261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L1221-L1261)

## Overview
Instructs the postmaster to terminate a background worker by setting a termination flag in shared memory and signaling the postmaster.

## Definition
void TerminateBackgroundWorker(BackgroundWorkerHandle *handle)

## Detailed Description
This function provides a safe mechanism to request termination of a background worker from any process. It operates by setting a terminate flag in the worker's shared memory slot and notifying the postmaster about the state change. The function uses generation numbers to ensure it only affects the intended worker instance, even if the slot has been reused for a different worker.

The function is designed to be safe to call regardless of the worker's current state - whether it's running, stopped, or even already unregistered. The actual termination is handled asynchronously by the postmaster, which will send appropriate signals to the worker process.

## Parameters / Member Variables
- `handle`: Pointer to BackgroundWorkerHandle identifying the worker to terminate

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (with BackgroundWorkerLock and LW_EXCLUSIVE)
  - LWLockRelease
  - SendPostmasterSignal (with PMSIGNAL_BACKGROUND_WORKER_CHANGE)
  - [BackgroundWorkerSlot](../B/BackgroundWorkerSlot.md) structure access
- Called from (representative examples):
  - DestroyParallelContext
  - [cleanup_background_workers](../c/cleanup_background_workers.md)

## Notes and Other Information
- Safe to call regardless of worker state (running, stopped, or unregistered)
- Uses exclusive locking to safely modify the terminate flag
- Uses generation numbers to prevent affecting wrong worker instances in reused slots
- Only signals postmaster if the generation matches (slot hasn't been reused)
- Termination is asynchronous - the function returns immediately after setting the flag
- Actual worker process termination is handled by the postmaster
- Located in src/backend/postmaster/bgworker.c:1221-1261