# WaitForBackgroundWorkerShutdown

## Location
src/backend/postmaster/bgworker.c: 1182 - 1220

## Overview
Waits for a background worker to completely stop, blocking until the worker transitions to STOPPED status regardless of its initial state.

## Definition
BgwHandleStatus WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *handle)

## Detailed Description
This function provides a blocking wait mechanism for background worker shutdown. It continuously polls the worker state until it reaches BGWH_STOPPED status, regardless of whether the worker is currently not yet started, running, or in some other state. The function uses the PostgreSQL latch mechanism to sleep efficiently between state checks, being awakened when the worker state changes.

Like its startup counterpart, this function implements a safety mechanism to detect postmaster death and return BGWH_POSTMASTER_DIED if the postmaster terminates during the wait, since state change notifications depend on the postmaster.

## Parameters / Member Variables
- `handle`: Pointer to BackgroundWorkerHandle for the worker to wait for shutdown

## Dependencies
- Functions called/Symbols referenced:
  - GetBackgroundWorkerPid
  - CHECK_FOR_INTERRUPTS
  - WaitLatch (with MyLatch, WL_LATCH_SET, WL_POSTMASTER_DEATH)
  - ResetLatch
  - BgwHandleStatus enum values (BGWH_STOPPED, BGWH_POSTMASTER_DIED)
- Called from (representative examples):
  - WaitForParallelWorkersToExit

## Notes and Other Information
- Waits for shutdown regardless of initial worker state (not started, running, etc.)
- Requires caller to set their PID as the worker's bgw_notify_pid for prompt awakening
- Uses WAIT_EVENT_BGWORKER_SHUTDOWN for wait event tracking
- Handles interrupts via CHECK_FOR_INTERRUPTS() in the polling loop
- Returns BGWH_POSTMASTER_DIED if postmaster dies during wait
- Unlike the startup function, this does not return a PID since the worker is stopped
- Located in src/backend/postmaster/bgworker.c:1182-1220