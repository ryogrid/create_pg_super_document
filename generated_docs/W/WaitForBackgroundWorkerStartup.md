# WaitForBackgroundWorkerStartup

## Location
[src/backend/postmaster/bgworker.c:1137-1181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L1137-L1181)

## Overview
Waits for a background worker to start up, blocking until the worker transitions from NOT_YET_STARTED to either STARTED or STOPPED status.

## Definition
BgwHandleStatus WaitForBackgroundWorkerStartup(BackgroundWorkerHandle *handle, pid_t *pidp)

## Detailed Description
This function provides a blocking wait mechanism for background worker startup. Unlike GetBackgroundWorkerPid(), which returns immediately with the current status, this function will continuously poll the worker state until it is no longer in the NOT_YET_STARTED state. It uses the PostgreSQL latch mechanism to sleep efficiently between checks, being awakened when the worker state changes or when the postmaster dies.

The function implements a safety mechanism to detect postmaster death, returning BGWH_POSTMASTER_DIED if the postmaster terminates during the wait, since worker startup cannot proceed without the postmaster.

## Parameters / Member Variables
- `handle`: Pointer to BackgroundWorkerHandle for the worker to wait for
- `pidp`: Output parameter that receives the worker's PID if startup succeeds

## Dependencies
- Functions called/Symbols referenced:
  - [GetBackgroundWorkerPid](../G/GetBackgroundWorkerPid.md)
  - CHECK_FOR_INTERRUPTS
  - [WaitLatch](WaitLatch.md) (with MyLatch, WL_LATCH_SET, WL_POSTMASTER_DEATH)
  - [ResetLatch](../R/ResetLatch.md)
  - [BgwHandleStatus](../B/BgwHandleStatus.md) enum values (BGWH_STARTED, BGWH_NOT_YET_STARTED, BGWH_POSTMASTER_DIED)
- Called from (representative examples):
  - [worker_spi_launch](../w/worker_spi_launch.md)

## Notes and Other Information
- Never returns BGWH_NOT_YET_STARTED (always waits until state changes)
- Requires caller to set their PID as the worker's bgw_notify_pid for prompt awakening
- Uses WAIT_EVENT_BGWORKER_STARTUP for wait event tracking
- Handles interrupts via CHECK_FOR_INTERRUPTS() in the polling loop
- Returns BGWH_POSTMASTER_DIED if postmaster dies during wait
- Located in src/backend/postmaster/bgworker.c:1137-1181