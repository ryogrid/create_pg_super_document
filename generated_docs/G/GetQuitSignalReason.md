# GetQuitSignalReason

## Location
[src/backend/storage/ipc/pmsignal.c:229-246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L229-L246)

## Overview
Retrieves the reason for a system shutdown from shared memory, typically called by child processes when they receive a SIGQUIT signal.

## Definition


## Detailed Description
This function allows child processes to determine why they are being terminated when they receive a SIGQUIT signal from the postmaster. It reads the shutdown reason from the PMSignalState shared memory structure that was previously set by SetQuitSignalReason. The function includes extra safety checks since it is called from signal handlers, verifying that the process is running under a postmaster and that the shared memory state is valid. If these conditions are not met, it returns PMQUIT_NOT_SENT to indicate that no legitimate shutdown signal was sent.

## Parameters / Member Variables
- Returns: QuitSignalReason enum value indicating the shutdown reason
  - : postmaster hasn't sent SIGQUIT or invalid state
  - : shutdown due to backend crash
  - : shutdown due to immediate stop command

## Dependencies
- Functions called/Symbols referenced:
  - IsUnderPostmaster (global variable check)
  - PMSignalState (global shared memory structure)
  - PMQUIT_NOT_SENT (enum constant)
- Called from (representative examples):
  - [quickdie](../q/quickdie.md) (src/backend/tcop/postgres.c:2949)

## Notes and Other Information
- Designed to be signal-safe with extra paranoid safety checks
- Returns PMQUIT_NOT_SENT if not running under postmaster or if PMSignalState is NULL
- Used by signal handlers to determine appropriate shutdown behavior
- Located in src/backend/storage/ipc/pmsignal.c:229-246
- Complements SetQuitSignalReason in the postmaster signaling mechanism