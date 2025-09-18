# index_delete_prefetch_buffer

## Location
src/backend/access/heap/heapam.c: 7990 - 8034

## Overview
A helper function for heap_index_delete_tuples that issues buffer prefetch requests to improve performance during bulk index tuple deletion operations.

## Definition
```c
static void index_delete_prefetch_buffer(Relation rel,
                                        IndexDeletePrefetchState *prefetch_state,
                                        int prefetch_count)
```

## Detailed Description
This function optimizes bulk index tuple deletion by issuing prefetch requests for heap page buffers that will be needed during the deletion process. It works with an IndexDeletePrefetchState structure that tracks the current position in the deletion array and maintains state between multiple prefetch calls.

The function processes the deltids array (which must be sorted by heap block number with all TIDs for each block grouped together) and issues prefetch requests for up to prefetch_count distinct heap blocks. It skips duplicate block numbers within the same group and maintains its position so that subsequent calls can continue where the previous call left off.

The prefetching is done using PostgreSQL's buffer prefetch mechanism (PrefetchBuffer) which asynchronously loads pages into the buffer pool, reducing I/O wait times when the actual deletion operations access those pages later.

## Parameters / Member Variables
- `rel`: The heap relation whose buffers are being prefetched
- `prefetch_state`: State structure tracking prefetch progress, including current position (next_item), current block number (cur_hblkno), and the deltids array
- `prefetch_count`: Maximum number of distinct heap blocks to prefetch in this call

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [PrefetchBuffer](../P/PrefetchBuffer.md)
  - MAIN_FORKNUM
  - TM_IndexDelete (structure)
  - IndexDeletePrefetchState (structure)
- Called from (representative examples):
  - [heap_index_delete_tuples](../h/heap_index_delete_tuples.md)

## Notes and Other Information
- This is a static helper function specifically designed for heap_index_delete_tuples
- Requires the deltids array to be pre-sorted by heap block number for optimal prefetch effectiveness
- The function maintains state between calls, allowing for incremental prefetching across multiple invocations
- Prefetching is only done once per distinct heap block to avoid redundant I/O requests
- Uses PostgreSQL's asynchronous buffer prefetch mechanism to overlap I/O with processing
- Critical for performance when deleting large numbers of index tuples that reference many different heap pages