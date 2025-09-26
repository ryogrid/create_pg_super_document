# find_multixact_start

## Location
[src/backend/access/transam/multixact.c:2880-2917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2880-L2917)

## Overview
Finds the starting member offset of a given MultiXactId by reading the corresponding offset page from the MultiXact SLRU.

## Definition

```c
static bool
find_multixact_start(MultiXactId multi, MultiXactOffset *result)
```
## Detailed Description
This function locates the starting member offset for a specified MultiXactId by reading from the MultiXact offset SLRU area. It first calculates the appropriate page number and entry number for the given MultiXactId, then ensures all dirty data is written out before checking if the required physical page exists on disk. If the page exists, it reads the page in read-only mode, extracts the offset from the appropriate entry, and returns it via the result parameter.

The function does not protect against concurrent truncation, so callers must handle that protection themselves if needed. This is a critical function for MultiXact member access and wraparound protection.

## Parameters / Member Variables
- : The MultiXactId whose starting offset should be found
- : Pointer to store the found starting member offset

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactIdToOffsetPage](../M/MultiXactIdToOffsetPage.md)
  - [MultiXactIdToOffsetEntry](../M/MultiXactIdToOffsetEntry.md)
  - [SimpleLruWriteAll](../S/SimpleLruWriteAll.md)
  - [SimpleLruDoesPhysicalPageExist](../S/SimpleLruDoesPhysicalPageExist.md)
  - [SimpleLruReadPage_ReadOnly](../S/SimpleLruReadPage_ReadOnly.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - MultiXactOffsetCtl
  - MultiXactMemberCtl
- Called from (representative examples):
  - debug_elog6 (src/backend/access/transam/multixact.c:414)
  - [SetOffsetVacuumLimit](../S/SetOffsetVacuumLimit.md) (src/backend/access/transam/multixact.c:2759)
  - [TruncateMultiXact](../T/TruncateMultiXact.md) (src/backend/access/transam/multixact.c:3177, 3195)

## Notes and Other Information
- Returns false if the file containing the multixact does not exist on disk
- Returns true and sets *result to the starting member offset if successful
- Requires MultiXactState->finishedStartup to be true (asserted)
- Writes out dirty data before checking physical page existence
- Uses read-only page access to avoid unnecessary locking overhead
- Critical for MultiXact member access and wraparound prevention
- Function is located at src/backend/access/transam/multixact.c:2880-2917