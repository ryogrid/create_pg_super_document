# closeLOfd

## Location
src/backend/libpq/be-fsstubs.c: 716 - 740

## Overview
Safely closes a large object file descriptor by cleaning up associated resources including snapshots and invoking the low-level close operation.

## Definition
```c
static void closeLOfd(int fd)
```

## Detailed Description
closeLOfd is a static utility function that performs safe cleanup of large object file descriptors. It implements a defensive cleanup strategy to prevent double-free errors:

1. Retrieves the `LargeObjectDesc` pointer from the cookies array at the specified file descriptor index
2. Immediately sets the cookies array slot to NULL to prevent accidental double-free attempts
3. If the large object has an associated snapshot, unregisters it from the TopTransactionResourceOwner
4. Calls `inv_close` to perform the actual low-level cleanup of the large object descriptor

The function prioritizes crash prevention over memory leak prevention by nullifying the cookies slot before cleanup, ensuring that even if subsequent operations fail, the slot won't be accessed again.

## Parameters / Member Variables
- `fd`: Integer file descriptor index into the cookies array identifying which large object to close

## Dependencies
- Functions called/Symbols referenced:
  - [LargeObjectDesc](../L/LargeObjectDesc.md) (struct type)
  - UnregisterSnapshotFromOwner
  - [inv_close](../i/inv_close.md)
- Called from (representative examples):
  - [be_lo_close](../b/be_lo_close.md) (src/backend/libpq/be-fsstubs.c:139)
  - [be_lo_unlink](../b/be_lo_unlink.md) (src/backend/libpq/be-fsstubs.c:341)
  - [AtEOXact_LargeObject](../A/AtEOXact_LargeObject.md) (src/backend/libpq/be-fsstubs.c:621)
  - [AtEOSubXact_LargeObject](../A/AtEOSubXact_LargeObject.md) (src/backend/libpq/be-fsstubs.c:665)

## Notes and Other Information
- Static function with file-local scope, used internally by large object operations
- Implements defensive programming by setting cookies[fd] to NULL before cleanup operations
- Handles snapshot cleanup for large objects that were opened with specific snapshot isolation
- Used by both explicit close operations (be_lo_close) and automatic cleanup during transaction end
- The comment "Better a leak than a crash" reflects PostgreSQL's philosophy of prioritizing stability
- Essential part of the large object resource management system, ensuring proper cleanup regardless of how the close is initiated