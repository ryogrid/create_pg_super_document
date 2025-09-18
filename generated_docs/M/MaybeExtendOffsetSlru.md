# MaybeExtendOffsetSlru

## Location
src/backend/access/transam/multixact.c: 2110 - 2144

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
This function takes no parameters and operates on global MultiXact state.

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactIdToOffsetPage](MultiXactIdToOffsetPage.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - LWLockAcquire
  - [SimpleLruDoesPhysicalPageExist](../S/SimpleLruDoesPhysicalPageExist.md)
  - [ZeroMultiXactOffsetPage](../Z/ZeroMultiXactOffsetPage.md)
  - [SimpleLruWritePage](../S/SimpleLruWritePage.md)
  - LWLockRelease
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