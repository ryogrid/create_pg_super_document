# CheckDeadLockAlert

## Location
src/backend/storage/lmgr/proc.c: 1845 - 1870

## Overview
Handles the expiry of deadlock_timeout by setting a global flag and triggering the process latch, designed to run safely within a signal handler context.

## Definition
void CheckDeadLockAlert(void)

## Detailed Description
CheckDeadLockAlert is a signal handler function that responds to deadlock timeout expiry. It operates in two phases: first, it sets the global flag got_deadlock_timeout to true to indicate that a deadlock timeout has occurred, then it sets the process latch (MyLatch) to wake up any waiting processes. The function is designed to be signal-safe, carefully preserving errno and using only async-signal-safe operations. The latch setting is done redundantly to ensure the process wakes up even if the latch was previously set before got_deadlock_timeout was updated.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SetLatch
- Called from (representative examples):
  - ProcessRecoveryConflictInterrupt
  - InitPostgres

## Notes and Other Information
- Runs inside a signal handler, so it must be async-signal-safe
- Sets got_deadlock_timeout flag before setting the latch to ensure proper signaling
- Preserves errno value to avoid interfering with interrupted code
- The redundant latch setting is intentional to handle race conditions
- May be called within procsignal_sigusr1_handler() context where the handler also sets the latch