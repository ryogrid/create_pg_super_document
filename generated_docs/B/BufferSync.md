# BufferSync

## Location
[src/backend/storage/buffer/bufmgr.c:2901-3176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2901-L3176)

## Overview
BufferSync writes out all dirty buffers in the shared buffer pool to disk, implementing the core checkpoint buffer synchronization with load balancing across tablespaces.

## Definition
```c
static void BufferSync(int flags)
```

## Detailed Description
BufferSync is the main function called during checkpoints to write all dirty shared buffers to disk. It uses a two-phase approach: first, it scans all buffers to identify dirty ones and marks them with BM_CHECKPOINT_NEEDED; then it writes the marked buffers in a carefully balanced manner across tablespaces using a binary heap. The function sorts buffers by tablespace, relation, fork, and block number to minimize random I/O. It implements sophisticated load balancing to prevent overwhelming individual tablespaces by writing proportionally from each tablespace based on their buffer counts. The function supports different checkpoint types through flags, writing additional buffer types during shutdown or recovery.

## Parameters / Member Variables
- `flags`: Checkpoint request flags that control behavior (CHECKPOINT_IMMEDIATE, CHECKPOINT_IS_SHUTDOWN, CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_FLUSH_ALL, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - [BufTagGetRelNumber](BufTagGetRelNumber.md)
  - [BufTagGetForkNum](BufTagGetForkNum.md)
  - sort_checkpoint_bufferids
  - [binaryheap_allocate](../b/binaryheap_allocate.md)
  - [binaryheap_add_unordered](../b/binaryheap_add_unordered.md)
  - [binaryheap_build](../b/binaryheap_build.md)
  - [binaryheap_first](../b/binaryheap_first.md)
  - [binaryheap_remove_first](../b/binaryheap_remove_first.md)
  - [binaryheap_replace_first](../b/binaryheap_replace_first.md)
  - [SyncOneBuffer](../S/SyncOneBuffer.md)
  - [CheckpointWriteDelay](../C/CheckpointWriteDelay.md)
  - [IssuePendingWritebacks](../I/IssuePendingWritebacks.md)
- Called from (representative examples):
  - [CheckPointBuffers](../C/CheckPointBuffers.md)
  - BufferIsPinned

## Notes and Other Information
- Uses BM_CHECKPOINT_NEEDED flag to track buffers that need writing during checkpoint
- Implements sophisticated tablespace load balancing using binary heap for fair I/O distribution
- Sorts buffers by tablespace, relation, fork, and block number to optimize disk access patterns
- Supports different checkpoint modes through flags (shutdown, recovery, immediate, etc.)
- Only writes permanent buffers during normal checkpoints, but writes all dirty buffers during shutdown/recovery
- Includes progress tracking and statistics collection for monitoring checkpoint performance
- Uses writeback context for efficient I/O batching and flushing

## Simplified Source

```c
// Simplified version of BufferSync
static void BufferSync(int flags) {
    int buf_id;
    int num_to_scan = 0;
    int num_written = 0;
    CkptTsStatus *per_ts_stat = NULL;
    binaryheap *ts_heap;
    int mask = BM_DIRTY;
    WritebackContext wb_context;

    // Determine which buffers to write based on checkpoint type
    if (!((flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FLUSH_ALL))))
        mask |= BM_PERMANENT;  // Only permanent buffers for normal checkpoints

    // Phase 1: Scan all buffers and mark dirty ones for checkpoint
    for (buf_id = 0; buf_id < NBuffers; buf_id++) {
        BufferDesc *bufHdr = GetBufferDescriptor(buf_id);

        uint32 buf_state = LockBufHdr(bufHdr);

        if ((buf_state & mask) == mask) {
            // Mark buffer as needing checkpoint and add to sort array
            buf_state |= BM_CHECKPOINT_NEEDED;

            CkptSortItem *item = &CkptBufferIds[num_to_scan++];
            item->buf_id = buf_id;
            item->tsId = bufHdr->tag.spcOid;
            item->relNumber = BufTagGetRelNumber(&bufHdr->tag);
            item->forkNum = BufTagGetForkNum(&bufHdr->tag);
            item->blockNum = bufHdr->tag.blockNum;
        }

        UnlockBufHdr(bufHdr, buf_state);
    }

    if (num_to_scan == 0)
        return;  // No dirty buffers to write

    WritebackContextInit(&wb_context, &checkpoint_flush_after);

    // Sort buffers by tablespace/relation/fork/block for efficient I/O
    sort_checkpoint_bufferids(CkptBufferIds, num_to_scan);

    // Set up per-tablespace progress tracking for load balancing
    int num_spaces = setup_tablespace_tracking(per_ts_stat, num_to_scan);

    // Create binary heap for balanced tablespace writing
    ts_heap = binaryheap_allocate(num_spaces, ts_ckpt_progress_comparator, NULL);

    for (int i = 0; i < num_spaces; i++) {
        CkptTsStatus *ts_stat = &per_ts_stat[i];
        ts_stat->progress_slice = (float8) num_to_scan / ts_stat->num_to_scan;
        binaryheap_add_unordered(ts_heap, PointerGetDatum(ts_stat));
    }
    binaryheap_build(ts_heap);

    // Phase 2: Write buffers in balanced order across tablespaces
    while (!binaryheap_empty(ts_heap)) {
        // Get next buffer from tablespace with least progress
        CkptTsStatus *ts_stat = (CkptTsStatus *) DatumGetPointer(binaryheap_first(ts_heap));
        buf_id = CkptBufferIds[ts_stat->index].buf_id;
        BufferDesc *bufHdr = GetBufferDescriptor(buf_id);

        // Write buffer if it still needs checkpoint
        if (pg_atomic_read_u32(&bufHdr->state) & BM_CHECKPOINT_NEEDED) {
            if (SyncOneBuffer(buf_id, false, &wb_context) & BUF_WRITTEN) {
                num_written++;
            }
        }

        // Update progress tracking and rebalance heap
        ts_stat->progress += ts_stat->progress_slice;
        ts_stat->num_scanned++;
        ts_stat->index++;

        if (ts_stat->num_scanned == ts_stat->num_to_scan) {
            binaryheap_remove_first(ts_heap);  // Tablespace complete
        } else {
            binaryheap_replace_first(ts_heap, PointerGetDatum(ts_stat));
        }

        // Throttle I/O rate based on checkpoint progress
        CheckpointWriteDelay(flags, (double) ts_stat->num_scanned / num_to_scan);
    }

    // Flush any pending writes and cleanup
    IssuePendingWritebacks(&wb_context, IOCONTEXT_NORMAL);
    pfree(per_ts_stat);
    binaryheap_free(ts_heap);

    CheckpointStats.ckpt_bufs_written += num_written;
}
```

Key simplifications made:
- Removed detailed comments and error handling for clarity
- Abstracted tablespace tracking setup into conceptual function call
- Simplified variable declarations and initialization
- Consolidated heap management operations
- Removed trace logging and barrier processing details
- Focused on the two-phase algorithm: mark dirty buffers, then write in balanced order
- Preserved essential load balancing and I/O optimization logic