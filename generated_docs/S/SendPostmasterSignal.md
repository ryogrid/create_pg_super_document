# SendPostmasterSignal

## Location
[src/backend/storage/ipc/pmsignal.c:181-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L181-L197)

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
- `reason`: PMSignalReason enum value indicating the specific type of signal/event to communicate to the postmaster
## Dependencies
- Functions called/Symbols referenced:
  - PMSignalReason (enum type for signal reasons)
  - IsUnderPostmaster (global variable check)
  - PMSignalState (global shared memory structure)
  - PostmasterPid (global variable with postmaster PID)
  - kill (system call to send signal)
  - SIGUSR1 (signal constant)
- Called from (representative examples):
  - [GetNewMultiXactId](../G/GetNewMultiXactId.md) (multixact ID management)
  - [GetNewTransactionId](../G/GetNewTransactionId.md) (transaction ID management)  
  - [do_start_worker](../d/do_start_worker.md) (autovacuum worker startup)
  - [RegisterDynamicBackgroundWorker](../R/RegisterDynamicBackgroundWorker.md) (background worker management)
  - [RequestXLogStreaming](../R/RequestXLogStreaming.md) (WAL streaming requests)
  - [pg_rotate_logfile](../p/pg_rotate_logfile.md) (log file rotation)

## Notes and Other Information
- This is a public function used throughout PostgreSQL for postmaster communication
- The function is safe to call from any backend process context
- Uses atomic flag setting in shared memory for thread/process safety
- The postmaster must have a SIGUSR1 handler that checks the PMSignalFlags array
- No-op when called from standalone backend (not running under postmaster)
- Part of PostgreSQL's inter-process communication infrastructure

## Simplified Source

```c
// Simplified version of SendPostmasterSignal
void SendPostmasterSignal(PMSignalReason reason) {
    // Skip if running as standalone backend (no postmaster to signal)
    if (!IsUnderPostmaster)
        return;

    // Set the specific signal flag in shared memory
    PMSignalState->PMSignalFlags[reason] = true;

    // Wake up the postmaster to check the flag
    kill(PostmasterPid, SIGUSR1);
}
```

Key simplifications made:
- Preserved the exact original logic as it was already quite simple
- Added clarifying comments for each major step
- This function is already well-optimized with minimal complexity
- The original 14-line implementation represents the essential algorithm clearly