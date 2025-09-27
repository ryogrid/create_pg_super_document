# SwitchToSharedLatch

## Location
[src/backend/utils/init/miscinit.c:222-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L222-L241)

## Overview
Switches the current process from using a process-local latch to using a shared latch stored in the process's PROC structure, enabling participation in shared memory-based signaling.

## Definition
```c
void SwitchToSharedLatch(void)
```

## Detailed Description
SwitchToSharedLatch transitions a PostgreSQL process from using its initial process-local latch to using a shared latch that is allocated in shared memory as part of the process's PROC structure. This transition is essential for processes that need to participate in shared memory-based inter-process communication and signaling mechanisms.

The function updates the global MyLatch pointer to reference the shared latch (MyProc->procLatch) and ensures that any existing wait event set is updated to monitor the new latch. It also sets the shared latch to preserve any pending signals that may have been delivered to the local latch.

This switch typically occurs after a process has been allocated a slot in the shared process array and is ready to participate in the broader PostgreSQL process coordination mechanisms.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md) (updates wait event set with new latch)
  - [SetLatch](SetLatch.md) (sets the shared latch state)
  - WL_LATCH_SET (wait event type constant)

- Called from (representative examples):
  - [InitProcess](../I/InitProcess.md)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md)

## Notes and Other Information
- Asserts that the current latch is the local latch (LocalLatchData) before switching
- Asserts that MyProc is not NULL, ensuring the process has a shared memory slot
- Updates the FeBeWaitSet if it exists to monitor the new shared latch
- Sets the shared latch to preserve any pending signals from the local latch
- Critical for enabling shared memory-based inter-process communication
- Part of the process initialization sequence for processes that participate in shared memory
- The switch is one-way; processes typically don't switch back to local latches once they've moved to shared latches

## Simplified Source

```c
// Simplified version of SwitchToSharedLatch
void SwitchToSharedLatch(void) {
    // Verify we're currently using the local latch and have a process slot
    Assert(MyLatch == &LocalLatchData);
    Assert(MyProc != NULL);

    // Switch to the shared latch in our process structure
    MyLatch = &MyProc->procLatch;

    // Update any existing wait event set to monitor the new latch
    if (FeBeWaitSet) {
        ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetLatchPos, WL_LATCH_SET, MyLatch);
    }

    // Set the shared latch to preserve any pending signals
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Preserved all essential assertions and logic flow
- Added descriptive comments for each major step
- Maintained the conditional check for FeBeWaitSet
- Kept the SetLatch call which ensures signal preservation
- Focused on the core functionality: switching from local to shared latch