# SendProcSignal

## Location
src/backend/storage/ipc/procsignal.c: 257 - 328

## Overview
Sends a signal to a PostgreSQL process by setting a signal flag in shared memory and delivering a SIGUSR1 signal, with optional process number optimization for faster lookups.

## Definition


## Detailed Description
SendProcSignal is the primary function for sending inter-process signals within PostgreSQL. It works by first setting a signal flag in the target process's shared memory slot, then sending a SIGUSR1 signal to wake up the target process. The function supports two modes of operation: if a valid procNumber is provided, it directly accesses the corresponding slot; otherwise, it searches through all slots to find the one matching the given PID. The search is performed backward to optimize for auxiliary processes that typically occupy slots near the end of the array. The function includes race condition handling and returns appropriate error codes.

## Parameters / Member Variables
- : Process ID of the target process to signal
- : The reason/type of signal being sent (from ProcSignalReason enum)
- : Optional process number for direct slot access (use INVALID_PROC_NUMBER to trigger PID search)

## Dependencies
- Functions called/Symbols referenced:
  - kill
  - pid_t (type)
  - ProcNumber (type)
  - ProcSignalReason (type)
  - ProcSignalSlot (type)
  - INVALID_PROC_NUMBER (constant)
  - SIGUSR1 (constant)
  - NumProcSignalSlots (variable)
- Called from (representative examples):
  - SignalBackends
  - SignalVirtualTransaction
  - CancelDBBackends
  - WalSndInitStopping
  - ParallelWorkerShutdown

## Notes and Other Information
- Returns 0 on success, -1 on error with errno set (typically ESRCH or EPERM)
- Handles race conditions gracefully - signals are designed to be safe if sent to wrong process
- Atomically sets signal flags before sending the actual signal
- Searches array backward when procNumber not provided to optimize for auxiliary processes
- Not to be confused with ProcSendSignal (different function)
- Signal flags are checked by target process in signal handler or polling loop
- Located in src/backend/storage/ipc/procsignal.c:257-328