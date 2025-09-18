# SwitchBackToLocalLatch

## Location
src/backend/utils/init/miscinit.c: 249 - 263

## Overview
Switches the current process from using a shared latch back to using a process-local latch, typically during process cleanup or when leaving shared memory participation.

## Definition
```c
void SwitchBackToLocalLatch(void)
```

## Detailed Description
SwitchBackToLocalLatch reverses the operation performed by SwitchToSharedLatch, transitioning a PostgreSQL process from using a shared memory-based latch back to using a process-local latch. This transition typically occurs during process cleanup when the process is about to exit or when it needs to detach from shared memory coordination mechanisms.

The function updates the global MyLatch pointer to reference the local LocalLatchData structure and ensures that any existing wait event set is updated to monitor the local latch instead of the shared one. It also sets the local latch to preserve any pending signals that may have been delivered to the shared latch.

This switch is typically performed as part of process cleanup sequences, particularly in ProcKill and AuxiliaryProcKill functions.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ModifyWaitEvent (updates wait event set with local latch)
  - SetLatch (sets the local latch state)
  - WL_LATCH_SET (wait event type constant)

- Called from (representative examples):
  - ProcKill
  - AuxiliaryProcKill

## Notes and Other Information
- Asserts that the current latch is not already the local latch before switching
- Asserts that MyProc is not NULL and MyLatch is the shared latch before switching
- Updates the FeBeWaitSet if it exists to monitor the local latch
- Sets the local latch to preserve any pending signals from the shared latch
- Part of the process cleanup sequence for processes leaving shared memory participation
- Enables continued latch functionality during process shutdown even after shared memory detachment
- Ensures proper state transition during process cleanup to avoid accessing freed shared memory
- The reverse operation of SwitchToSharedLatch, completing the latch lifecycle