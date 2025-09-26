# spgvacuumpage

## Location
[src/backend/access/spgist/spgvacuum.c:621-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L621-L691)

## Overview
Processes a single page during an SP-GiST index bulkdelete scan, handling different page types and performing vacuum operations as needed.

## Definition

```c
static void
spgvacuumpage(spgBulkDeleteState *bds, BlockNumber blkno)
```
## Detailed Description
The `spgvacuumpage` function is a core component of SP-GiST's vacuum operation that processes individual index pages during bulk deletion. It implements a state machine approach to handle different types of pages:

- **New/Empty pages**: Handles crash recovery scenarios and empty pages
- **Leaf pages**: Differentiates between root and non-root leaf pages, applying appropriate vacuum strategies
- **Inner pages**: Cleans up redirect and placeholder tuples

The function ensures proper buffer management, applies vacuum delay points for throttling, and maintains the Free Space Map (FSM) for efficient space utilization. It preserves the integrity of root pages by never marking them for deletion.

## Parameters / Member Variables
- `bds`: Bulk delete state containing vacuum statistics, heap relation info, and strategy information
- `blkno`: Block number of the page to be vacuumed

## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [vacuumLeafRoot](../v/vacuumLeafRoot.md)
  - [vacuumLeafPage](../v/vacuumLeafPage.md)
  - [vacuumRedirectAndPlaceholder](../v/vacuumRedirectAndPlaceholder.md)
  - [RecordFreeIndexPage](../R/RecordFreeIndexPage.md)
  - [SpGistSetLastUsedPage](../S/SpGistSetLastUsedPage.md)
  - [PageIsNew](../P/PageIsNew.md)/PageIsEmpty
  - SpGistPageIsLeaf/SpGistBlockIsRoot
- Called from (representative examples):
  - [spgvacuumscan](spgvacuumscan.md)

## Notes and Other Information
- Uses exclusive buffer locking to ensure data consistency during vacuum operations
- Implements vacuum delay points to prevent overwhelming the system during large vacuum operations
- Root pages are never deleted or marked as available in FSM to maintain index structure integrity
- Updates vacuum statistics including pages_deleted counter
- Maintains lastFilledBlock tracking for efficient space management