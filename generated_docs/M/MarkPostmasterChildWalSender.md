# MarkPostmasterChildWalSender

## Location
src/backend/storage/ipc/pmsignal.c: 339 - 355

## Overview
Marks a postmaster child process as a WAL sender, transitioning the child's status from active to WAL sender state.

## Definition


## Detailed Description
This function is called by child processes to signal that they have become WAL sender processes. It updates the child's status in the PMSignalState structure from PM_CHILD_ACTIVE to PM_CHILD_WALSENDER. The function operates on the child's own slot (identified by MyPMChildSlot) in the shared memory array that tracks child process states.

This represents a specialized transition in the child process lifecycle. WAL senders start as regular active processes but upgrade to WAL sender status once they begin streaming WAL data. The WALSENDER state is like ACTIVE but carries additional semantic meaning that the process is specifically handling WAL replication.

The function includes an assertion to verify that the calling process is indeed a WAL sender (am_walsender flag), ensuring this transition only occurs in the appropriate context.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : Global variable containing the child's assigned slot number
- : Shared memory array tracking child process states
- : Global flag indicating if the current process is a WAL sender

## Dependencies
- Constants used:
  - PM_CHILD_ACTIVE (value 2)
  - PM_CHILD_WALSENDER (value 3)
- Global variables accessed:
  - MyPMChildSlot
  - PMSignalState
  - am_walsender
- Called from:
  - Currently no direct references found in the indexed codebase

## Notes and Other Information
- The function includes assertions to verify the slot is valid, the process is a WAL sender, and the child is in the expected ACTIVE state
- Slot numbers are 1-based externally but converted to 0-based for array indexing
- This is part of the postmaster-child communication mechanism used for process lifecycle tracking
- Must be called from WAL sender child processes only, after they have already been marked as active
- WAL senders never transition back to ACTIVE state once they become WALSENDER