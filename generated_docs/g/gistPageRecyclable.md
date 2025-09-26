# gistPageRecyclable

## Location
[src/backend/access/gist/gistutil.c:887-910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L887-L910)

## Overview
gistPageRecyclable determines whether a GiST index page can be safely recycled for reuse by checking if it's safe from concurrent access considerations.

## Definition
```c
bool gistPageRecyclable(Page page)
```

## Detailed Description
This function implements the safety logic for page recycling in GiST indexes. It returns true if a page can be safely reused without affecting concurrent operations. The function handles three cases: (1) newly allocated but uninitialized pages are always recyclable, (2) deleted pages are recyclable only if their deletion transaction is no longer visible to any active transaction (ensuring no concurrent scan can access them), and (3) all other pages are not recyclable. This implements a tombstone mechanism where deleted pages must remain accessible until all transactions that might reference them have completed.

## Parameters / Member Variables
- `page`: Page pointer to the page being checked for recyclability

## Dependencies
- Functions called/Symbols referenced:
  - [PageIsNew](../P/PageIsNew.md) (to check if page is uninitialized)
  - GistPageIsDeleted (to check if page is marked as deleted)
  - [GistPageGetDeleteXid](../G/GistPageGetDeleteXid.md) (to get the transaction ID that deleted the page)
  - [FullTransactionId](../F/FullTransactionId.md) (transaction ID type)
  - [GlobalVisCheckRemovableFullXid](../G/GlobalVisCheckRemovableFullXid.md) (to check if deletion XID is still visible to any transaction)
- Called from (representative examples):
  - [gistNewBuffer](gistNewBuffer.md) (during page allocation to check if FSM pages can be reused)
  - [gistvacuumpage](gistvacuumpage.md) (during vacuum operations to determine page recyclability)

## Notes and Other Information
- Critical for maintaining MVCC (Multi-Version Concurrency Control) semantics in GiST indexes
- Implements a tombstone mechanism where deleted pages remain accessible until safe to recycle
- Prevents race conditions where concurrent scans might access recycled pages
- Used in conjunction with the Free Space Map (FSM) for efficient space management
- The visibility check ensures that no active snapshot can still see the deleted page
- Essential for both correctness and performance of GiST index operations
- Part of PostgreSQL's transaction visibility infrastructure