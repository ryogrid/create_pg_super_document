# gistNewBuffer

## Location
[src/backend/access/gist/gistutil.c:823-886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L823-L886)

## Overview
gistNewBuffer allocates a new page for a GiST index by either recycling a deleted page from the Free Space Map (FSM) or extending the index file with a new page.

## Definition
```c
Buffer gistNewBuffer(Relation r, Relation heaprel)
```

## Detailed Description
This function implements a sophisticated page allocation strategy for GiST indexes. It first attempts to recycle pages from the Free Space Map (FSM) by checking if they are safe to reuse (either never initialized or properly deleted and old enough). If no suitable pages are available for recycling, it extends the index file with a new page. The function handles concurrency by using conditional locking and validates page integrity. For Hot Standby scenarios, it generates appropriate WAL records when recycling pages to handle query conflicts.

## Parameters / Member Variables
- `r`: Relation pointer representing the GiST index relation
- `heaprel`: Relation pointer to the corresponding heap relation (used for WAL generation in Hot Standby)

## Dependencies
- Functions called/Symbols referenced:
  - [GetFreeIndexPage](../G/GetFreeIndexPage.md) (to get candidate pages from FSM)
  - [ReadBuffer](../R/ReadBuffer.md) (to read pages from disk)
  - [ConditionalLockBuffer](../C/ConditionalLockBuffer.md) (for non-blocking buffer locking)
  - [BufferGetPage](../B/BufferGetPage.md) (to extract page from buffer)
  - [PageIsNew](../P/PageIsNew.md) (to check if page is uninitialized)
  - [gistcheckpage](gistcheckpage.md) (to validate page integrity)
  - [gistPageRecyclable](gistPageRecyclable.md) (to check if page can be recycled)
  - XLogStandbyInfoActive (to check if Hot Standby WAL is needed)
  - RelationNeedsWAL (to check if relation requires WAL logging)
  - [gistXLogPageReuse](gistXLogPageReuse.md) (to generate WAL record for page reuse)
  - [GistPageGetDeleteXid](../G/GistPageGetDeleteXid.md) (to get deletion transaction ID)
  - [LockBuffer](../L/LockBuffer.md) (to unlock buffer when recycling fails)
  - [ReleaseBuffer](../R/ReleaseBuffer.md) (to release unusable buffers)
  - [ExtendBufferedRel](../E/ExtendBufferedRel.md) (to extend the index file)
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md) (during tuple insertion and page splits)
  - [gistbuild](gistbuild.md) (during index construction)

## Notes and Other Information
- Returns a buffer that is already pinned and exclusive-locked
- Caller must initialize the returned buffer using GISTInitBuffer
- Implements efficient page recycling to reduce index file growth
- Handles Hot Standby scenarios with appropriate WAL logging for query conflict resolution
- Uses conditional locking to avoid blocking when other processes are using candidate pages
- Falls back to file extension when no recyclable pages are available
- Critical for maintaining good space utilization in GiST indexes