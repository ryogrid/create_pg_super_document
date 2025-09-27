# SwitchBackToLocalLatch

## Location
[src/backend/utils/init/miscinit.c:249-263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L249-L263)

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
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md) (updates wait event set with local latch)
  - [SetLatch](SetLatch.md) (sets the local latch state)
  - WL_LATCH_SET (wait event type constant)

- Called from (representative examples):
  - [ProcKill](../P/ProcKill.md)
  - [AuxiliaryProcKill](../A/AuxiliaryProcKill.md)

## Notes and Other Information
- Asserts that the current latch is not already the local latch before switching
- Asserts that MyProc is not NULL and MyLatch is the shared latch before switching
- Updates the FeBeWaitSet if it exists to monitor the local latch
- Sets the local latch to preserve any pending signals from the shared latch
- Part of the process cleanup sequence for processes leaving shared memory participation
- Enables continued latch functionality during process shutdown even after shared memory detachment
- Ensures proper state transition during process cleanup to avoid accessing freed shared memory
- The reverse operation of SwitchToSharedLatch, completing the latch lifecycle

## Simplified Source

```c
// Simplified version of SwitchBackToLocalLatch
void SwitchBackToLocalLatch(void) {
    // Verify we're currently using a shared latch
    Assert(MyLatch != &LocalLatchData);
    Assert(MyProc != NULL && MyLatch == &MyProc->procLatch);

    // Switch back to the local latch
    MyLatch = &LocalLatchData;

    // Update wait event set if it exists
    if (FeBeWaitSet) {
        ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetLatchPos, WL_LATCH_SET, MyLatch);
    }

    // Activate the local latch to preserve any pending signals
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Added clear comments explaining each logical step
- Preserved the essential assertion checks for correctness
- Maintained the core logic flow: verify state, switch latch, update wait events, set latch
- Focused on the main execution path without removing any actual functionality