# XLogPrefetcherAddFilter

## Location
[src/backend/access/transam/xlogprefetcher.c:858-895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L858-L895)

## Overview
Adds a filter to prevent prefetching blocks from a specific relation starting at a given block number until a specified LSN has been replayed during WAL recovery.

## Definition

```c
static inline void
XLogPrefetcherAddFilter(XLogPrefetcher *prefetcher, RelFileLocator rlocator,
						BlockNumber blockno, XLogRecPtr lsn)
```
## Detailed Description
This function manages the prefetch filter system that prevents premature prefetching of blocks that may not yet be valid for reading. It maintains a hash table of active filters keyed by , where each filter specifies a block range and LSN threshold.

The filtering mechanism is essential for correctness during WAL replay, as it prevents the prefetcher from attempting to read:
- Blocks from relations that don't exist yet (due to pending creation operations)
- Blocks beyond the current size of relations (due to pending extension operations)
- Blocks from relations that will be truncated (until the truncation is replayed)
- Blocks from databases created using the file-copy strategy

When a filter already exists for a relation, the function extends the filter's lifetime to cover the new LSN while keeping the most restrictive block number (minimum of existing and new block numbers).

## Parameters / Member Variables
- : Pointer to the XLogPrefetcher structure containing the filter infrastructure
- : RelFileLocator identifying the specific relation (tablespace, database, relation)
- : Starting block number from which to apply the filter (blocks >= blockno will be filtered)
- : LSN that must be replayed before the filter can be removed

## Dependencies
- Functions called/Symbols referenced:
  -  - [Hash](../H/Hash.md) table operations for filter management
  -  - Add filter to the active filter queue
  -  - Remove filter from queue for reordering
  -  - [Hash](../H/Hash.md) table insertion flag
- Called from (representative examples):
  -  - Multiple call sites for different filtering scenarios

## Notes and Other Information
- Filters are stored in both a hash table (for fast lookup) and a doubly-linked list (for ordered processing)
- When extending an existing filter, it maintains the most restrictive block number to ensure safety
- The filter queue is ordered by insertion, with newer filters at the head
- Critical for preventing data corruption during crash recovery scenarios
- Inline function for performance in the hot path of WAL prefetching