# SendPostmasterSignal

## Location
src/backend/storage/ipc/pmsignal.c: 181 - 197

## Overview
Sends a signal from a backend child process to the postmaster process, using shared memory flags and SIGUSR1 to communicate specific events or requests.

## Definition

```c
void
SendPostmasterSignal(PMSignalReason reason)
```
## Detailed Description
SendPostmasterSignal provides a mechanism for child processes to communicate with the postmaster process. The function works by:

1. First checking if the process is running under a postmaster (returns early for standalone backends)
2. Atomically setting a specific flag in the shared memory PMSignalState structure based on the provided reason
3. Sending a SIGUSR1 signal to the postmaster process to notify it that a signal flag has been set

This two-step approach (shared memory flag + signal) allows multiple types of signals to be communicated using a single OS signal, with the specific reason encoded in shared memory flags that the postmaster can check upon receiving SIGUSR1.

## Parameters / Member Variables
- : PMSignalReason enum value indicating the specific type of signal/event to communicate to the postmaster

## Dependencies
- Functions called/Symbols referenced:
  - PMSignalReason (enum type for signal reasons)
  - IsUnderPostmaster (global variable check)
  - PMSignalState (global shared memory structure)
  - PostmasterPid (global variable with postmaster PID)
  - kill (system call to send signal)
  - SIGUSR1 (signal constant)
- Called from (representative examples):
  - GetNewMultiXactId (multixact ID management)
  - GetNewTransactionId (transaction ID management)  
  - do_start_worker (autovacuum worker startup)
  - RegisterDynamicBackgroundWorker (background worker management)
  - RequestXLogStreaming (WAL streaming requests)
  - pg_rotate_logfile (log file rotation)

## Notes and Other Information
- This is a public function used throughout PostgreSQL for postmaster communication
- The function is safe to call from any backend process context
- Uses atomic flag setting in shared memory for thread/process safety
- The postmaster must have a SIGUSR1 handler that checks the PMSignalFlags array
- No-op when called from standalone backend (not running under postmaster)
- Part of PostgreSQL's inter-process communication infrastructure