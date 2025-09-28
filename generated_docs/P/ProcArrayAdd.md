# ProcArrayAdd

## Location
[src/backend/storage/ipc/procarray.c:468-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L468-L564)

## Overview
Adds a specified PGPROC structure to the shared process array, maintaining the array in sorted order for optimal cache locality during traversals.

## Definition

```c
structs too, and so we should have failed
		 * earlier.)
		 */
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("sorry, too many clients already")));
```
## Detailed Description
ProcArrayAdd inserts a new process entry into the shared process array (procArray). The function maintains the array sorted by PGPROC number to optimize cache locality when traversing the array. It acquires both ProcArrayLock and XidGenLock to ensure atomic updates to all related structures.

The function performs several key operations:
1. Finds the correct insertion point to maintain sorted order
2. Shifts existing entries to make room for the new process
3. Updates not only the pgprocnos array but also corresponding entries in xids, subxidStates, and statusFlags arrays
4. Adjusts the pgxactoff field for all affected processes

The sorted arrangement improves performance during frequent operations like snapshot building and visibility checking, where the process array is traversed regularly.

## Parameters / Member Variables
- : Pointer to the PGPROC structure to be added to the shared array

## Dependencies
- Functions called/Symbols referenced:
  - GetNumberFromPGProc
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - ereport
  - memmove
  - [ProcArrayStruct](ProcArrayStruct.md)
  - ProcGlobal
  - NUM_AUXILIARY_PROCS

- Called from (representative examples):
  - [InitProcessPhase2](../I/InitProcessPhase2.md)
  - [MarkAsPrepared](../M/MarkAsPrepared.md)

## Notes and Other Information
- Requires exclusive locks on both ProcArrayLock and XidGenLock to prevent race conditions
- The function will terminate the process with FATAL error if the array is full (should not happen in normal operation)
- Maintains sorted order by PGPROC number for cache efficiency
- Updates multiple parallel arrays (pgprocnos, xids, subxidStates, statusFlags) atomically
- Adjusts pgxactoff values for all processes that are shifted in the array
- Lock release order is reversed from acquisition order to minimize lock contention
- The sorting overhead is justified because array access is much more frequent than addition/removal

## Simplified Source

```c
// Simplified version of ProcArrayAdd
void ProcArrayAdd(PGPROC *proc) {
    int proc_number = GetNumberFromPGProc(proc);
    ProcArrayStruct *array = procArray;
    int insertion_index;
    int items_to_move;

    // Acquire exclusive locks for atomic update
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

    // Check if array has space (should always succeed)
    if (array->numProcs >= array->maxProcs) {
        ereport(FATAL, (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                       errmsg("sorry, too many clients already")));
    }

    // Find correct insertion point to maintain sorted order
    for (insertion_index = 0; insertion_index < array->numProcs; insertion_index++) {
        int current_proc_number = array->pgprocnos[insertion_index];
        if (current_proc_number > proc_number)
            break;  // Found insertion point
    }

    // Shift existing entries to make room for new process
    items_to_move = array->numProcs - insertion_index;
    memmove(&array->pgprocnos[insertion_index + 1],
            &array->pgprocnos[insertion_index],
            items_to_move * sizeof(*array->pgprocnos));

    // Also shift corresponding data in parallel arrays
    memmove(&ProcGlobal->xids[insertion_index + 1],
            &ProcGlobal->xids[insertion_index],
            items_to_move * sizeof(*ProcGlobal->xids));
    // ... similar moves for subxidStates and statusFlags arrays

    // Insert new process data at correct position
    array->pgprocnos[insertion_index] = proc_number;
    proc->pgxactoff = insertion_index;
    ProcGlobal->xids[insertion_index] = proc->xid;
    ProcGlobal->subxidStates[insertion_index] = proc->subxidStatus;
    ProcGlobal->statusFlags[insertion_index] = proc->statusFlags;

    array->numProcs++;

    // Update pgxactoff for all processes that were shifted
    for (int i = insertion_index + 1; i < array->numProcs; i++) {
        int shifted_proc_number = array->pgprocnos[i];
        allProcs[shifted_proc_number].pgxactoff = i;
    }

    // Release locks in reverse order to minimize contention
    LWLockRelease(XidGenLock);
    LWLockRelease(ProcArrayLock);
}
```

Key simplifications made:
- Removed detailed comments and assertions for clarity
- Used more descriptive variable names (e.g., `insertion_index` instead of `index`)
- Consolidated similar memmove operations with ellipsis notation
- Simplified the loop logic for finding insertion point
- Removed platform-specific details and focused on core algorithm
- Abstracted low-level memory operations with high-level comments
- Maintained the essential sorted-array insertion algorithm