# MaybeExtendOffsetSlru

## Location
[src/backend/access/transam/multixact.c:2110-2144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2110-L2144)

## Overview
MaybeExtendOffsetSlru extends the MultiXact offsets SLRU area when necessary, particularly to handle missing pages after binary upgrades from PostgreSQL 9.2 and earlier.

## Definition
```c
static void MaybeExtendOffsetSlru(void)
```

## Detailed Description
This static function addresses a specific compatibility issue that arises after binary upgrades from PostgreSQL versions 9.2 and earlier. In those older versions, the on-disk representation of MultiXact data was different, and pg_multixact/offsets files might be shorter than required for the current system state.

During upgrade, pg_upgrade updates pg_control to set the next MultiXact offset value to preserve the visibility of tuples locked by existing MultiXacts. However, if the old installation had used MultiXacts beyond the first page, the corresponding pages might not exist in the new format. This function ensures that any missing pages needed for the next MultiXact ID are created.

The function calculates which page is needed for the next MultiXact ID, acquires the appropriate lock, checks if the physical page exists, and if not, creates and writes it to disk. The operation is safe because SimpleLruWritePage can handle creating new segment files even when the page being written is not the first in the segment.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactIdToOffsetPage](MultiXactIdToOffsetPage.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [SimpleLruDoesPhysicalPageExist](../S/SimpleLruDoesPhysicalPageExist.md)
  - [ZeroMultiXactOffsetPage](../Z/ZeroMultiXactOffsetPage.md)
  - [SimpleLruWritePage](../S/SimpleLruWritePage.md)
  - [LWLockRelease](../L/LWLockRelease.md)
- Global variables accessed:
  - MultiXactState
  - MultiXactOffsetCtl
- Called from:
  - [MultiXactSetNextMXact](MultiXactSetNextMXact.md)

## Notes and Other Information
- Function is static and only accessible within the multixact.c module
- Specifically designed to handle binary upgrade compatibility issues
- Only creates pages that don't physically exist, avoiding unnecessary work
- Uses exclusive locking to ensure atomic page creation
- Critical for proper functioning after upgrades from PostgreSQL 9.2 and earlier
- The function is safe to call multiple times as it only acts when pages are missing
- Does not write XLOG records since this is a recovery/upgrade scenario

## Simplified Source

```c
// Simplified version of MaybeExtendOffsetSlru
static void MaybeExtendOffsetSlru(void) {
    // Calculate which page is needed for the next MultiXact ID
    int64 pageno = MultiXactIdToOffsetPage(MultiXactState->nextMXact);

    // Get the bank lock for this page to ensure exclusive access
    LWLock *lock = SimpleLruGetBankLock(MultiXactOffsetCtl, pageno);
    LWLockAcquire(lock, LW_EXCLUSIVE);

    // Check if the physical page exists on disk
    if (!SimpleLruDoesPhysicalPageExist(MultiXactOffsetCtl, pageno)) {
        // Page doesn't exist - create it
        // Zero out a new page and write it to disk
        int slotno = ZeroMultiXactOffsetPage(pageno, false);
        SimpleLruWritePage(MultiXactOffsetCtl, slotno);
    }

    // Release the exclusive lock
    LWLockRelease(lock);
}
```

Key simplifications made:
- Removed detailed comments explaining binary upgrade background
- Simplified variable declarations and consolidated logic flow
- Added inline comments explaining each major step
- Preserved essential algorithm: calculate page → lock → check existence → create if missing → unlock
- Maintained all critical function calls and error handling logic
- Focused on the core page creation workflow rather than upgrade context