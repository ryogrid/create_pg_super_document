# ApplyLauncherWakeup

## Location
[src/backend/replication/logical/launcher.c:1125-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L1125-L1134)

## Overview
Sends a SIGUSR1 signal to the logical replication launcher process to wake it up from its waiting state.

## Definition

```c
static void
ApplyLauncherWakeup(void)
```
## Detailed Description
This static function implements the actual mechanism for waking up the logical replication launcher process. It checks if a launcher process is currently running by examining the launcher_pid field in the LogicalRepCtx shared memory structure. If a valid PID exists (non-zero), it sends a SIGUSR1 signal to that process using the kill() system call. This signal interrupts the launcher's sleep/wait state and prompts it to check for new work, such as starting or restarting subscription workers. The function is designed to be safe to call even when no launcher is running.

## Parameters / Member Variables
(This function takes no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - kill (system call)
  - SIGUSR1 (signal constant)
  - LogicalRepCtx->launcher_pid (shared memory field)
- Called from (representative examples):
  - [logicalrep_worker_onexit](../l/logicalrep_worker_onexit.md)
  - [AtEOXact_ApplyLauncher](AtEOXact_ApplyLauncher.md)

## Notes and Other Information
- This is a static function, only accessible within the launcher.c file
- The function safely handles the case where no launcher process exists (PID is 0)
- SIGUSR1 is the standard signal used by PostgreSQL for inter-process communication
- This function is typically called when subscription workers exit or when transactions that modify subscriptions commit
- The launcher process must have a signal handler registered for SIGUSR1 to properly respond to the wakeup