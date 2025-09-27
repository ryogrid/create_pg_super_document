# AssignPostmasterChildSlot

## Location
[src/backend/storage/ipc/pmsignal.c:247-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L247-L283)

## Overview
Selects an unused slot for a new postmaster child process and marks it as assigned, returning the allocated slot number.

## Definition

```c
int
AssignPostmasterChildSlot(void)
```
## Detailed Description
This function manages the allocation of slots in the PMChildFlags array for new child processes spawned by the postmaster. It searches for a free slot starting from the last assigned position to avoid repeatedly scanning low-numbered slots, improving efficiency. When a free slot is found, it marks both the local PMChildInUse array and the shared memory PMChildFlags array to indicate the slot is assigned. The function is designed to be called only by the postmaster process, eliminating the need for special locking mechanisms. It uses a circular scanning approach to distribute slot usage evenly and returns a 1-based slot number for the allocated slot.

## Parameters / Member Variables
- Returns:  - slot number (1 to N) of the assigned slot

## Dependencies
- Functions called/Symbols referenced:
  - next_child_inuse (static variable tracking last assigned slot)
  - num_child_inuse (static variable for total available slots)
  - PMChildInUse (local array tracking slot usage)
  - PMSignalState->PMChildFlags (shared memory slot state array)
  - PM_CHILD_ASSIGNED (enum constant for assigned state)
  - elog (error logging function)
- Called from (representative examples):
  - [BackendStartup](../B/BackendStartup.md) (src/backend/postmaster/postmaster.c:3587)
  - [StartAutovacuumWorker](../S/StartAutovacuumWorker.md) (src/backend/postmaster/postmaster.c:3996)
  - [assign_backendlist_entry](../a/assign_backendlist_entry.md) (src/backend/postmaster/postmaster.c:4388)

## Notes and Other Information
- Only the postmaster process should call this function (no locking needed)
- Uses circular scanning starting from the last assigned slot for efficiency
- Returns 1-based slot numbers (adds 1 to internal 0-based index)
- Will terminate with FATAL error if no free slots are available
- Part of the postmaster child process management system
- Located in src/backend/storage/ipc/pmsignal.c:247-283
- Tracks both local (PMChildInUse) and shared memory (PMChildFlags) state

## Simplified Source

```c
// Simplified version of AssignPostmasterChildSlot
int AssignPostmasterChildSlot(void) {
    int slot = next_child_inuse;
    int n;

    // Scan for a free slot using circular search starting from last assigned position
    for (n = num_child_inuse; n > 0; n--) {
        // Wrap around to end if we reach the beginning
        if (--slot < 0)
            slot = num_child_inuse - 1;

        // Found a free slot - mark it as assigned
        if (!PMChildInUse[slot]) {
            PMChildInUse[slot] = true;
            PMSignalState->PMChildFlags[slot] = PM_CHILD_ASSIGNED;
            next_child_inuse = slot;
            return slot + 1;  // Return 1-based slot number
        }
    }

    // All slots are in use - this should never happen
    elog(FATAL, "no free slots in PMChildFlags array");
    return 0;
}
```

Key simplifications made:
- Condensed the verbose comment about trusting PMSignalState contents
- Simplified the slot assignment logic flow with clearer comments
- Focused on the core circular search algorithm
- Maintained the essential error handling for the no-slots-available case
- Preserved the 1-based return value conversion