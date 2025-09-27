# MarkPostmasterChildInactive

## Location
[src/backend/storage/ipc/pmsignal.c:356-375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L356-L375)

## Overview
Marks a postmaster child process as done using shared memory, transitioning the child's status back to assigned state during cleanup.

## Definition

```c
void
MarkPostmasterChildInactive(void)
```
## Detailed Description
This function is called by child processes to signal that they are finished using shared memory and are preparing to exit. It updates the child's status in the PMSignalState structure from either PM_CHILD_ACTIVE or PM_CHILD_WALSENDER back to PM_CHILD_ASSIGNED. The function operates on the child's own slot (identified by MyPMChildSlot) in the shared memory array that tracks child process states.

This represents the cleanup phase in the child process lifecycle. When a child process is done with its work and ready to exit, it calls this function to indicate that its shared memory resources can be considered available for cleanup. The process transitions back to ASSIGNED state, indicating it's no longer actively using shared memory but the slot is still associated with the process until the postmaster performs final cleanup.

The function accepts transitions from both ACTIVE and WALSENDER states, allowing both regular backend processes and WAL sender processes to properly clean up.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : Global variable containing the child's assigned slot number
- : Shared memory array tracking child process states

## Dependencies
- Constants used:
  - PM_CHILD_ACTIVE (value 2)
  - PM_CHILD_WALSENDER (value 3)
  - PM_CHILD_ASSIGNED (value 1)
- Global variables accessed:
  - MyPMChildSlot
  - PMSignalState
- Called from:
  - [ProcKill](../P/ProcKill.md) (src/backend/storage/lmgr/proc.c:959)

## Notes and Other Information
- The function includes assertions to verify the slot is valid and the child is in either ACTIVE or WALSENDER state
- Slot numbers are 1-based externally but converted to 0-based for array indexing
- This is part of the postmaster-child communication mechanism used for process lifecycle tracking
- Must be called from child processes during their cleanup/exit sequence
- After this call, the process should not access shared memory structures

## Simplified Source

```c
// Simplified version of MarkPostmasterChildInactive
void MarkPostmasterChildInactive(void) {
    // Get our assigned slot number (convert from 1-based to 0-based)
    int slot = MyPMChildSlot - 1;

    // Verify slot is valid and process is currently active
    // (either regular active or WAL sender process)

    // Mark this child process as no longer active
    // Transition back to ASSIGNED state for cleanup
    PMSignalState->PMChildFlags[slot] = PM_CHILD_ASSIGNED;
}
```

Key simplifications made:
- Removed detailed assertion checks for clarity
- Combined slot calculation into single step
- Added explanatory comments for each logical step
- Focused on the core state transition logic
- Abstracted validation details while preserving intent