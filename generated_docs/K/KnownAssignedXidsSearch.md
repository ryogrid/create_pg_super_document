# KnownAssignedXidsSearch

## Location
[src/backend/storage/ipc/procarray.c:4885-4972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4885-L4972)

## Overview
Searches the KnownAssignedXids array for a specific transaction ID and optionally removes it, using binary search algorithm for efficient lookup.

## Definition

```c
static bool
KnownAssignedXidsSearch(TransactionId xid, bool remove)
```
## Detailed Description
KnownAssignedXidsSearch performs a binary search on the sorted KnownAssignedXids array to locate a specific transaction ID. The function can operate in two modes: search-only mode and search-and-remove mode. In search-only mode, it simply checks for the existence of the transaction ID. In search-and-remove mode, it marks the found entry as invalid and updates the array management pointers.

The function implements optimizations for array management, particularly when removing the tail element by advancing the tail pointer over invalid entries to speed up future searches. When the array becomes empty after removal, both head and tail pointers are reset to 0.

The search ignores the validity status during the binary search phase since even invalid entries contain sorted XIDs, but validates the entry before returning true.

## Parameters / Member Variables
- : The transaction ID to search for in the KnownAssignedXids array
- : Boolean flag indicating whether to remove the transaction ID if found (true) or just search (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md)
  - pg_read_barrier
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
- Called from (representative examples):
  - [KnownAssignedXidExists](KnownAssignedXidExists.md)
  - [KnownAssignedXidsRemove](KnownAssignedXidsRemove.md)

## Notes and Other Information
- Caller must hold ProcArrayLock in shared mode for search-only operations or exclusive mode for remove operations
- Uses binary search algorithm for O(log n) time complexity
- Only the startup process removes entries, so read barriers are skipped during removal operations
- Automatically advances tail pointer when removing tail elements to optimize future searches
- Returns false for both non-existent XIDs and invalid entries in the array