# visibilitymap_set

## Location
[src/backend/access/heap/visibilitymap.c:244-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/visibilitymap.c#L244-L335)

## Overview
Sets visibility map bits for a previously pinned page, handling WAL logging and maintaining consistency between heap pages and their visibility metadata.

## Definition

```c
void
visibilitymap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
				  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
				  uint8 flags)
```
## Detailed Description
The visibilitymap_set function completes the second phase of visibility map bit setting operations. It sets the specified visibility bits in a pre-pinned visibility map page, ensuring proper WAL logging for crash recovery and Hot Standby support. The function includes comprehensive validation to ensure data integrity and handles different scenarios for normal operation versus recovery.

The function performs critical section protection during bit manipulation and handles WAL logging with special considerations for data checksums and hint bits. When setting all-visible bits, it ensures the corresponding heap page has the PD_ALL_VISIBLE bit set before proceeding. The cutoff_xid parameter supports Hot Standby by providing the oldest transaction ID that can see all tuples on the page.

## Parameters / Member Variables
- `rel`: The relation whose visibility map is being updated
- `heapBlk`: Block number of the heap page whose visibility bits are being set
- `heapBuf`: Buffer containing the heap page (required for WAL logging, except in recovery)
- `recptr`: LSN of the XLOG record being replayed (InvalidXLogRecPtr in normal operation)
- `vmBuf`: Pre-pinned buffer containing the correct visibility map page
- `cutoff_xid`: Largest xmin on the page (for Hot Standby, can be InvalidTransactionId)
- `flags`: Bitmask specifying which visibility bits to set
## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_MAPBLOCK/HEAPBLK_TO_MAPBYTE/HEAPBLK_TO_OFFSET (heap-to-map conversion macros)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets block number from buffer)
  - [PageGetContents](../P/PageGetContents.md) (gets page contents from buffer)
  - [PageIsAllVisible](../P/PageIsAllVisible.md) (checks if heap page has all-visible bit set)
  - [log_heap_visible](../l/log_heap_visible.md) (generates WAL record for visibility changes)
  - XLogHintBitIsNeeded (determines if hint bit protection is needed)
  - RelationNeedsWAL (checks if relation requires WAL logging)
- Called from (representative examples):
  - [heap_multi_insert](../h/heap_multi_insert.md) (sets all-visible bits after bulk inserts)
  - [lazy_scan_prune](../l/lazy_scan_prune.md) (sets visibility bits during vacuum operations)
  - [heap_xlog_visible](../h/heap_xlog_visible.md) (sets bits during WAL replay)

## Notes and Other Information
- Must be called with buffers previously pinned via visibilitymap_pin
- Requires heap page to have PD_ALL_VISIBLE bit set before calling (except in recovery)
- Handles both normal operation (generates WAL) and recovery mode (replays existing WAL)
- Uses critical sections to ensure atomicity of visibility map updates
- Special handling for data checksums: updates heap page LSN only when hint bits are protected
- Validates that all_frozen bit is never set without all_visible bit
- No-op if the requested bits are already set

## Simplified Source

```c
void
visibilitymap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
                  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
                  uint8 flags)
{
    // Convert heap block to visibility map coordinates
    BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
    uint32 mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
    uint8 mapOffset = HEAPBLK_TO_OFFSET(heapBlk);

    // Validate input parameters
    Assert(InRecovery || XLogRecPtrIsInvalid(recptr));
    Assert(InRecovery || PageIsAllVisible(BufferGetPage(heapBuf)));
    Assert((flags & VISIBILITYMAP_VALID_BITS) == flags);
    Assert(flags != VISIBILITYMAP_ALL_FROZEN);  // Can't set frozen without visible

    // Validate correct buffers are provided
    if (BufferIsValid(heapBuf) && BufferGetBlockNumber(heapBuf) != heapBlk)
        elog(ERROR, "wrong heap buffer passed to visibilitymap_set");
    if (!BufferIsValid(vmBuf) || BufferGetBlockNumber(vmBuf) != mapBlock)
        elog(ERROR, "wrong VM buffer passed to visibilitymap_set");

    // Access visibility map page and lock it
    Page page = BufferGetPage(vmBuf);
    uint8 *map = (uint8 *) PageGetContents(page);
    LockBuffer(vmBuf, BUFFER_LOCK_EXCLUSIVE);

    // Check if bits need to be set (avoid redundant work)
    if (flags != (map[mapByte] >> mapOffset & VISIBILITYMAP_VALID_BITS)) {
        START_CRIT_SECTION();

        // Set the visibility bits
        map[mapByte] |= (flags << mapOffset);
        MarkBufferDirty(vmBuf);

        // Handle WAL logging if needed
        if (RelationNeedsWAL(rel)) {
            if (XLogRecPtrIsInvalid(recptr)) {
                // Normal operation: generate new WAL record
                recptr = log_heap_visible(rel, heapBuf, vmBuf, cutoff_xid, flags);

                // Update heap page LSN if checksums/hints need protection
                if (XLogHintBitIsNeeded()) {
                    Page heapPage = BufferGetPage(heapBuf);
                    PageSetLSN(heapPage, recptr);
                }
            }
            // Update visibility map page LSN
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
    }

    LockBuffer(vmBuf, BUFFER_LOCK_UNLOCK);
}
```