# _bt_allocbuf

## Location
src/backend/access/nbtree/nbtpage.c: 869 - 1002

## Overview
_bt_allocbuf allocates a new block/page for a B-tree index, either by reusing a page from the Free Space Map (FSM) or by extending the relation with a new page.

## Definition
```c
Buffer _bt_allocbuf(Relation rel, Relation heaprel)
```

## Detailed Description
This function implements a sophisticated page allocation strategy for B-tree indexes. It first attempts to reuse pages from the FSM, but includes careful safeguards to handle race conditions and deadlocks. The function uses conditional locking to avoid deadlocks when trying to reuse FSM pages, and includes proper WAL logging for Hot Standby conflict detection when reusing pages.

The allocation process follows these steps:
1. Query FSM for potentially free pages
2. Attempt conditional locking on reported pages to avoid deadlocks
3. Verify pages are actually recyclable using BTPageIsRecyclable
4. Generate WAL records for Hot Standby conflict detection if needed
5. Fall back to extending the relation if no suitable pages are found

The function handles edge cases like all-zeroes pages (from crashed backends) and ensures proper initialization of allocated pages.

## Parameters / Member Variables
- `rel`: The B-tree index relation to allocate a page for
- `heaprel`: The associated heap relation (required for generating snapshotConflictHorizon for Hot Standby safety)

## Dependencies
- Functions called/Symbols referenced:
  - GetFreeIndexPage (queries FSM for free pages)
  - ReadBuffer (reads candidate pages into buffer pool)
  - _bt_conditionallockbuf (attempts conditional locking to avoid deadlocks)
  - PageIsNew (checks for all-zeroes pages)
  - BTPageIsRecyclable (verifies page can be safely reused)
  - XLogBeginInsert, XLogRegisterData, XLogInsert (WAL logging for page reuse)
  - ExtendBufferedRel (extends relation when no reusable pages available)
  - _bt_pageinit (initializes allocated pages)
  - _bt_relbuf (releases non-suitable pages)
- Called from (representative examples):
  - _bt_split (during page splitting operations)
  - _bt_newlevel (when creating new B-tree levels)
  - _bt_getroot (when allocating initial root page)

## Notes and Other Information
- Uses conditional locking strategy to prevent deadlocks with concurrent operations
- Includes sophisticated handling of race conditions with FSM and VACUUM
- Generates WAL records for Hot Standby conflict detection when reusing pages
- Falls back to relation extension when FSM pages aren't suitable
- Handles edge case of all-zeroes pages from backend crashes
- Returns a write-locked buffer containing an initialized, empty B-tree page
- Located in src/backend/access/nbtree/nbtpage.c:869-1002