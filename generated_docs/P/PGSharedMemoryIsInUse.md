# PGSharedMemoryIsInUse

## Location
[src/backend/port/sysv_shmem.c:317-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L317-L346)

## Overview
PGSharedMemoryIsInUse checks whether a previously-existing shared memory segment is still in use, primarily to detect crashed postmaster processes with surviving child backends.

## Definition
```c
bool PGSharedMemoryIsInUse(unsigned long id1, unsigned long id2)
```

## Detailed Description
This function determines if a shared memory segment from a previous PostgreSQL instance is still active and in use. Its primary purpose is crash recovery detection - specifically to identify cases where a postmaster process crashed but left child backend processes still running and attached to shared memory.

The function works by:
1. Attempting to attach to the specified shared memory segment using PGSharedMemoryAttach
2. Immediately detaching from the segment if attachment was successful  
3. Analyzing the attachment state to determine if the segment is actively in use
4. Returning true if the segment appears to be in use by PostgreSQL processes

The function only considers segments associated with the intended DataDir to avoid false positives from coincidental shared memory ID matches, which can occur in practice.

## Parameters / Member Variables
- `id1`: First identifier (currently unused in the implementation)
- `id2`: The shared memory segment ID to check

## Dependencies
- Functions called/Symbols referenced:
  - [PGSharedMemoryAttach](PGSharedMemoryAttach.md)
  - shmdt (System V IPC function)
  - elog (PostgreSQL logging)
  - IpcMemoryState enum values (SHMSTATE_ENOENT, SHMSTATE_FOREIGN, etc.)
- Called from (representative examples):
  - [CreateLockFile](../C/CreateLockFile.md)

## Notes and Other Information
- This is a public function accessible from other PostgreSQL modules
- The id1 parameter is present in the signature but not used in the current implementation
- The function safely detaches from any segment it successfully attaches to for testing
- Used during PostgreSQL startup to detect potential conflicts with existing instances
- Returns false for states indicating the segment is not in use (ENOENT, FOREIGN, UNATTACHED)
- Returns true for states indicating active use (ANALYSIS_FAILURE, ATTACHED) with a conservative default
- Critical for preventing multiple PostgreSQL instances from conflicting over the same data directory

## Simplified Source

```c
// Simplified version of PGSharedMemoryIsInUse
bool PGSharedMemoryIsInUse(unsigned long id1, unsigned long id2) {
    PGShmemHeader *memAddress;
    IpcMemoryState state;

    // Try to attach to the shared memory segment to check its state
    state = PGSharedMemoryAttach((IpcMemoryId) id2, NULL, &memAddress);

    // If we successfully attached, immediately detach to avoid interference
    if (memAddress && shmdt((void *) memAddress) < 0)
        elog(LOG, "shmdt(%p) failed: %m", memAddress);

    // Check the attachment state to determine if segment is in use
    switch (state) {
        case SHMSTATE_ENOENT:      // Segment doesn't exist
        case SHMSTATE_FOREIGN:     // Segment exists but not PostgreSQL
        case SHMSTATE_UNATTACHED:  // Segment exists but no processes attached
            return false;

        case SHMSTATE_ANALYSIS_FAILURE:  // Can't determine state - assume in use
        case SHMSTATE_ATTACHED:          // Active PostgreSQL processes attached
            return true;
    }

    // Conservative default: assume segment is in use
    return true;
}
```

Key simplifications made:
- Added explanatory comments for each major step
- Clarified the purpose of each state case with inline comments
- Grouped similar return cases together for better readability
- Preserved the essential logic flow and error handling
- Maintained the conservative default behavior