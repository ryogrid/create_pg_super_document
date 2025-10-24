# mdexists

## Location
[src/backend/storage/smgr/md.c:171-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L171-L189)

## Overview
mdexists checks whether a physical file exists for a specific relation fork, ensuring accurate existence detection by closing the fork first to detect any pending deletions.

## Definition
bool mdexists(SMgrRelation reln, ForkNumber forknum)

## Detailed Description
This function determines if a physical file exists for a given relation fork by attempting to open it. To ensure accurate detection, it first closes the fork (unless in recovery mode) to guarantee that any pending unlink operations are properly detected. The function returns true even for "lingering files" that have pending deletions but haven't been physically removed yet. As an optimization, the close operation is skipped during recovery since the recovery process already handles relation cleanup when dropping them.

## Parameters / Member Variables
- : SMgrRelation representing the storage manager relation
- : ForkNumber specifying which fork of the relation to check (main, FSM, VM, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [mdclose](mdclose.md) (closes the fork to ensure fresh state)
  - [mdopenfork](mdopenfork.md) (attempts to open the fork to test existence)
  - EXTENSION_RETURN_NULL (flag for mdopenfork to return NULL instead of extending)
  - InRecovery (global variable indicating recovery mode)

- Called from (representative examples):
  - Declared in src/include/storage/md.h for external usage
  - Used by higher-level storage management code to check file existence

## Notes and Other Information
- Returns true for lingering files with pending deletions, which may be counter-intuitive
- Skips the close operation during recovery for performance optimization
- The close-then-open sequence ensures that cached file descriptors don't mask deleted files
- Uses EXTENSION_RETURN_NULL flag to prevent mdopenfork from creating files if they don't exist
- Part of the magnetic disk storage manager's file existence checking mechanism

## Simplified Source

```c
bool mdexists(SMgrRelation reln, ForkNumber forknum)
{
    // Close the fork first to detect any pending unlinks (except during recovery)
    if (!InRecovery)
        mdclose(reln, forknum);

    // Try to open the fork - if successful, the file exists
    return (mdopenfork(reln, forknum, EXTENSION_RETURN_NULL) != NULL);
}
```

**Key Points:**
- Checks if a physical file exists for a specific relation fork
- Closes the fork first (unless in recovery) to ensure fresh state detection
- Returns true even for lingering files with pending deletions
- Uses mdopenfork with EXTENSION_RETURN_NULL to avoid creating files
- Optimization: skips close during recovery since relations are already cleaned up