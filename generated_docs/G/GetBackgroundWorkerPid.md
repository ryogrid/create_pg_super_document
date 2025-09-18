# GetBackgroundWorkerPid

## Location
src/backend/postmaster/bgworker.c: 1082 - 1136

## Overview
Retrieves the process ID (PID) of a dynamically-registered background worker and returns its current status.

## Definition


## Detailed Description
This function determines the current state of a background worker process and optionally returns its PID. It examines the worker slot associated with the given handle and checks whether the worker is running, not yet started, or stopped. The function uses lightweight locking to safely access shared worker data structures and handles various worker lifecycle states including temporary stops (for restart), permanent stops, and unregistered workers.

The function implements atomic reads of the worker PID while acknowledging that the value may become stale by the time the caller uses it, which is an inherent limitation in concurrent systems.

## Parameters / Member Variables
- : Pointer to BackgroundWorkerHandle containing slot information and generation number for the target worker
- : Output parameter that receives the worker's PID if the worker is currently running

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (with BackgroundWorkerLock and LW_SHARED)
  - LWLockRelease
  - [BackgroundWorkerSlot](../B/BackgroundWorkerSlot.md) structure access
  - [BgwHandleStatus](../B/BgwHandleStatus.md) enum values (BGWH_STARTED, BGWH_NOT_YET_STARTED, BGWH_STOPPED)
- Called from (representative examples):
  - WaitForParallelWorkersToAttach
  - [WaitForBackgroundWorkerStartup](../W/WaitForBackgroundWorkerStartup.md)
  - [WaitForBackgroundWorkerShutdown](../W/WaitForBackgroundWorkerShutdown.md)
  - [shm_mq_counterparty_gone](../s/shm_mq_counterparty_gone.md)

## Notes and Other Information
- Returns BGWH_STARTED if worker is running (with PID in *pidp)
- Returns BGWH_NOT_YET_STARTED if postmaster hasn't attempted to start the worker yet
- Returns BGWH_STOPPED for various termination scenarios including temporary stops, permanent stops, or unregistered workers
- Uses generation numbers to detect slot reuse and validate handle freshness
- Implements simple locking strategy rather than memory barriers for data synchronization
- Located in src/backend/postmaster/bgworker.c:1082-1136