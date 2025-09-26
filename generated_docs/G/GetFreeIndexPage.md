# GetFreeIndexPage

## Location
[src/backend/storage/freespace/indexfsm.c:38-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/indexfsm.c#L38-L51)

## Overview
GetFreeIndexPage returns a free page from the Free Space Map (FSM) for index operations and marks it as used as a side effect.

## Definition
```c
BlockNumber GetFreeIndexPage(Relation rel)
```

## Detailed Description
GetFreeIndexPage is a core function in PostgreSQL's index free space management system. It requests a free page from the Free Space Map that has at least BLCKSZ/2 (half a block) of free space available. The function serves as a high-level interface for index access methods to obtain new pages for index expansion.

As a crucial side effect, when a valid page is found, the function immediately marks it as used in the FSM by calling RecordUsedIndexPage. This prevents race conditions where multiple processes might attempt to use the same "free" page simultaneously.

The function uses a conservative threshold of BLCKSZ/2 to ensure that the returned page has sufficient free space for typical index operations, avoiding situations where a page might be technically "free" but lack adequate space for meaningful use.

## Parameters / Member Variables
- `rel`: The Relation structure representing the index for which a free page is needed

## Dependencies
- Functions called/Symbols referenced:
  - [GetPageWithFreeSpace](GetPageWithFreeSpace.md) (requests a page with at least BLCKSZ/2 free space)
  - [RecordUsedIndexPage](../R/RecordUsedIndexPage.md) (marks the returned page as used in FSM)
- Called from (representative examples):
  - [GinNewBuffer](GinNewBuffer.md) (GIN index buffer allocation)
  - [gistNewBuffer](../g/gistNewBuffer.md) (GiST index buffer allocation)
  - [_bt_allocbuf](../b/_bt_allocbuf.md) (B-tree page allocation)
  - [SpGistNewBuffer](../S/SpGistNewBuffer.md) (SP-GiST buffer allocation)

## Notes and Other Information
- Returns InvalidBlockNumber if no suitable free page is available
- The BLCKSZ/2 threshold ensures reasonable free space availability
- Automatically handles FSM bookkeeping by marking pages as used
- Used by all major index access methods in PostgreSQL
- Part of the centralized index free space management infrastructure