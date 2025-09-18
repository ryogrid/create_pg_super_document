# ReportBackgroundWorkerPID

## Location
src/backend/postmaster/bgworker.c: 467 - 485

## Overview
Reports the PID of a newly-launched background worker in shared memory, allowing other processes to track the worker's status and optionally notify the requesting process.

## Definition


## Detailed Description
This function updates the shared memory slot with the process ID of a background worker that has just been successfully started. It stores the PID in the appropriate BackgroundWorkerSlot and optionally sends a SIGUSR1 signal to notify the process that requested the background worker (if bgw_notify_pid was specified). This function is designed to be called exclusively from the postmaster process after a background worker has been forked and launched.

## Parameters / Member Variables
- `rw`: Pointer to RegisteredBgWorker structure containing the worker's registration information, shared memory slot index, and actual PID

## Dependencies
- Functions called/Symbols referenced:
  - kill (system call for sending signals)
  - SIGUSR1 (signal constant)
- Data structures used:
  - [RegisteredBgWorker](RegisteredBgWorker.md)
  - [BackgroundWorkerSlot](../B/BackgroundWorkerSlot.md)
  - BackgroundWorkerData (global shared memory structure)
- Called from (representative examples):
  - [BackgroundWorkerStateChange](../B/BackgroundWorkerStateChange.md)
  - [do_start_bgworker](../d/do_start_bgworker.md)

## Notes and Other Information
- This function should only be called from the postmaster process
- Includes an assertion to ensure the shared memory slot index is within valid bounds (< max_worker_processes)
- The notification mechanism (SIGUSR1) allows processes to be immediately informed when their requested background worker has started
- Part of PostgreSQL's background worker management system that coordinates worker lifecycle between postmaster and requesting processes