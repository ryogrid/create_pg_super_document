# MaybeStartWalReceiver

## Location
[src/backend/postmaster/postmaster.c:4053-4071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4053-L4071)

## Overview
MaybeStartWalReceiver conditionally starts a WAL receiver process if none is currently running and the postmaster state allows for recovery operations.

## Definition

```c
static void
MaybeStartWalReceiver(void)
```
## Detailed Description
MaybeStartWalReceiver implements conditional startup logic for WAL receiver processes in PostgreSQL. It starts a WAL receiver only when specific conditions are met: no receiver is currently running (WalReceiverPID == 0), the postmaster is in a recovery-related state (PM_STARTUP, PM_RECOVERY, or PM_HOT_STANDBY), and the system is not in an immediate shutdown mode.

The function includes important race condition handling - it doesn't clear the WalReceiverRequested flag if a receiver is already running, because the receiver might terminate just as a new request arrives. This approach prefers launching an extra receiver (which will detect it's not needed and exit) over missing a needed receiver startup.

If the receiver starts successfully, the function clears the WalReceiverRequested flag. If startup fails, the flag remains set so the system will retry later during subsequent ServerLoop iterations or signal processing.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [StartChildProcess](../S/StartChildProcess.md) (creates the WAL receiver process)
- [Variables](../V/Variables.md) referenced:
  - WalReceiverPID (tracks current receiver process ID)
  - pmState (postmaster state - checked against PM_STARTUP, PM_RECOVERY, PM_HOT_STANDBY)  
  - Shutdown (shutdown state - compared with SmartShutdown)
  - WalReceiverRequested (flag indicating receiver startup was requested)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (main postmaster loop for regular checks)
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md) (signal handler for receiver startup requests)

## Notes and Other Information
- Implements race condition prevention by allowing potential extra receiver launches
- Only starts receivers during recovery-related postmaster states (startup, recovery, hot standby)
- Respects shutdown modes - won't start receivers during immediate shutdown
- Uses B_WAL_RECEIVER backend type for the child process
- Designed to be called repeatedly - handles its own state checking and flag management
- WAL receivers have built-in logic to detect when they're not needed and exit gracefully