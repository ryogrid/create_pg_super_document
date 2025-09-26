# InitializeTimeouts

## Location
src/backend/utils/misc/timeout.c: 470 - 504

## Overview
Initializes the PostgreSQL timeout module, setting up all internal data structures and establishing the SIGALRM signal handler for timeout processing.

## Definition
```c
void InitializeTimeouts(void)
```

## Detailed Description
The `InitializeTimeouts` function performs complete initialization of PostgreSQL timeout management system. This function must be called in every process that intends to use timeouts, including backend processes, background workers, and utility processes.

The initialization process includes:
1. **Alarm Disabling**: Temporarily disables any existing alarms to ensure clean state
2. **State Reset**: Resets the count of active timeouts to zero
3. **Timeout Array Initialization**: Initializes all timeout slots in the `all_timeouts` array, setting each timeout to inactive state with cleared handler pointers and timing information
4. **Module State Flag**: Sets the `all_timeouts_initialized` flag to indicate the module is ready
5. **Signal Handler Registration**: Establishes `handle_sig_alarm` as the handler for SIGALRM signals

This function is fork-safe and should be called after process forking but before re-enabling signals to avoid inheriting parent process signal handlers.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm: Disables alarm system temporarily
  - MAX_TIMEOUTS: Maximum number of concurrent timeouts supported
  - pqsignal: PostgreSQL signal registration function
  - SIGALRM: POSIX alarm signal constant
  - handle_sig_alarm: The signal handler function for timeout processing
- Called from (representative examples):
  - AutoVacWorkerMain: Autovacuum worker process initialization
  - BackgroundWorkerMain: Background worker process initialization  
  - StartupProcessMain: Database startup process initialization
  - BackendInitialize: Backend process initialization
  - PostgresMain: Main postgres process initialization
  - WalSndSignals: WAL sender process signal setup

## Notes and Other Information
- Must be called in every process that uses timeouts before any timeout registration
- Fork-safe: should be called after forking but before enabling signals in child processes
- Initializes up to MAX_TIMEOUTS concurrent timeouts
- Sets up the global signal handler for SIGALRM which is shared across all timeout instances
- Critical for proper timeout functionality in all PostgreSQL processes including backends, background workers, and utility processes
- The function ensures clean state by clearing all timeout slots regardless of previous state