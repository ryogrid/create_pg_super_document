# XLogPrefetcherFilter

## Location
[src/backend/access/transam/xlogprefetcher.c:160-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L160-L166)

## Overview
XLogPrefetcherFilter is a temporary filtering mechanism used to track and suppress prefetching of block ranges and relations that haven't been created yet, have been dropped, or will be created by bulk WAL operations.

## Definition


## Detailed Description
The XLogPrefetcherFilter serves as a selective blocking mechanism within the WAL prefetching system to prevent attempts to prefetch blocks that are not yet valid or accessible. It maintains temporal and spatial filtering rules: temporal by tracking when (at which LSN) the filter should be lifted, and spatial by defining which blocks (from a specific block number onwards) should be filtered. The structure is organized in a double-linked list for efficient queue management and stored in a hash table for fast lookup by relation file locator.

## Parameters / Member Variables
- : RelFileLocator identifying the specific file (relation/table) that this filter applies to
- : XLogRecPtr (LSN) position that must be replayed before this filter is removed
- : BlockNumber specifying the first block number to filter (all blocks >= this number are filtered)
- : dlist_node for linking this filter into the prefetcher's filter queue for ordering and management

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (file location identifier)
  - XLogRecPtr (WAL position type)  
  - BlockNumber (block number type)
  - [dlist_node](../d/dlist_node.md) (double-linked list node)

- Called from (representative examples):
  - [XLogPrefetcherAllocate](XLogPrefetcherAllocate.md) (sets up hash table for filters)
  - [XLogPrefetcherAddFilter](XLogPrefetcherAddFilter.md) (adds new filter or updates existing one)
  - [XLogPrefetcherCompleteFilters](XLogPrefetcherCompleteFilters.md) (removes completed/expired filters)
  - [XLogPrefetcherIsFiltered](XLogPrefetcherIsFiltered.md) (checks if a block should be filtered)

## Notes and Other Information
The filtering mechanism supports both specific block range filtering and whole database filtering (by setting appropriate RelFileLocator fields to invalid values). Filters are managed in a FIFO queue and indexed by a hash table for O(1) lookup performance. The system handles filter lifetime extension - when multiple WAL records affect the same relation, the filter duration is extended to cover the latest LSN, and the block range is adjusted to cover the minimum (most restrictive) block number. This prevents premature prefetching of blocks that may cause IO errors or unnecessary work when the blocks don't yet exist in the target relation.