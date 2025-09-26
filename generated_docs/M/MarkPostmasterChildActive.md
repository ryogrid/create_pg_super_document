# MarkPostmasterChildActive

## Location
[src/backend/storage/ipc/pmsignal.c:323-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L323-L338)

## Overview
Marks a postmaster child process as actively using shared memory, transitioning the child's status from assigned to active state.

## Definition

```c
void
MarkPostmasterChildActive(void)
```
## Detailed Description
This function is called by child processes to signal that they are about to begin actively using shared memory. It updates the child's status in the PMSignalState structure from PM_CHILD_ASSIGNED to PM_CHILD_ACTIVE. The function operates on the child's own slot (identified by MyPMChildSlot) in the shared memory array that tracks child process states.

The PM signal system maintains per-child-process flags with four possible states:
- UNUSED: Available for assignment
- ASSIGNED: Associated with a process but not yet actively using shared memory
- ACTIVE: Process is actively using shared memory  
- WALSENDER: Special active state for WAL sender processes

This function specifically handles the ASSIGNED → ACTIVE transition, which occurs when a child process is ready to begin normal operations involving shared memory access.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : Global variable containing the child's assigned slot number
- : Shared memory array tracking child process states

## Dependencies
- Constants used:
  - PM_CHILD_ASSIGNED (value 1)
  - PM_CHILD_ACTIVE (value 2)
- Global variables accessed:
  - MyPMChildSlot
  - PMSignalState
- Called from:
  - [InitProcess](../I/InitProcess.md) (src/backend/storage/lmgr/proc.c:378)

## Notes and Other Information
- The function includes assertions to verify the slot is valid and the child is in the expected ASSIGNED state
- Slot numbers are 1-based externally but converted to 0-based for array indexing
- This is part of the postmaster-child communication mechanism used for process lifecycle tracking
- Must be called from child processes only, not from the postmaster itself