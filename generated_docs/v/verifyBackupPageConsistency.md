# verifyBackupPageConsistency

## Location
[src/backend/access/transam/xlogrecovery.c:2461-2572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2461-L2572)

## Overview
Validates the consistency between the current buffer page and backup page stored in WAL records after replay operations have completed.

## Definition
```c
static void verifyBackupPageConsistency(XLogReaderState *record)
```

## Detailed Description
The `verifyBackupPageConsistency` function performs critical data integrity validation during WAL recovery by comparing the current state of database pages with their backup images stored in WAL records. This function is called after WAL replay operations to ensure that the applied changes match exactly with what was originally recorded, providing a crucial safety mechanism against data corruption.

The function iterates through all block references in a WAL record, reads both the current page content from buffers and the backup image from the WAL record, applies optional masking to ignore non-critical differences (like hint bits and unused space), and performs a byte-by-byte comparison. If inconsistencies are detected, the function terminates the recovery process with a fatal error.

## Parameters / Member Variables
- `record`: Pointer to XLogReaderState containing the WAL record to verify for consistency

## Dependencies
- Functions called/Symbols referenced:
  - [GetRmgr](../G/GetRmgr.md): Gets the resource manager for the record type
  - XLogRecGetRmid: Extracts resource manager ID from the record
  - XLogRecHasAnyBlockRefs: Checks if the record contains block references
  - XLogRecGetInfo: Gets the info field from the record
  - XLogRecMaxBlockId: Gets the maximum block ID in the record
  - [XLogRecGetBlockTagExtended](../X/XLogRecGetBlockTagExtended.md): Extracts block location information
  - XLogRecHasBlockImage: Checks if block has a backup image
  - XLogRecBlockImageApply: Checks if block image was already applied
  - [XLogReadBufferExtended](../X/XLogReadBufferExtended.md): Reads the current page from buffer
  - [LockBuffer](../L/LockBuffer.md)/UnlockReleaseBuffer: Buffer locking operations
  - [BufferGetPage](../B/BufferGetPage.md): Gets page from buffer
  - [PageGetLSN](../P/PageGetLSN.md): Gets the Log Sequence Number from page
  - [RestoreBlockImage](../R/RestoreBlockImage.md): Restores backup image from WAL record
  - memcmp: Compares the two page images
- Constants used:
  - XLR_CHECK_CONSISTENCY: Flag indicating consistency check needed
  - RBM_NORMAL_NO_LOG: Buffer read mode
  - BUFFER_LOCK_EXCLUSIVE: Exclusive buffer lock mode
  - BLCKSZ: Block size constant
- Called from:
  - [ApplyWalRecord](../A/ApplyWalRecord.md): Main WAL replay function that calls this for verification

## Notes and Other Information
- This is a static function, only accessible within xlogrecovery.c
- Only processes records that have the XLR_CHECK_CONSISTENCY flag set
- Skips verification if the record has no block references or if the block image was already applied
- Uses masking functions (rm_mask) to ignore non-critical page differences like hint bits and unused space
- Terminates recovery with FATAL error if page inconsistency is detected
- Critical for ensuring data integrity during crash recovery and replication scenarios
- The function uses global buffers (replay_image_masked, primary_image_masked) for page comparisons
- Skips blocks where the page LSN is ahead of the current record's EndRecPtr (indicating recovery restart)

## Simplified Source

```c
static void verifyBackupPageConsistency(XLogReaderState *record)
{
    RmgrData rmgr = GetRmgr(XLogRecGetRmid(record));
    RelFileLocator rlocator;
    ForkNumber forknum;
    BlockNumber blkno;

    // Skip if record has no block references
    if (!XLogRecHasAnyBlockRefs(record))
        return;

    Assert((XLogRecGetInfo(record) & XLR_CHECK_CONSISTENCY) != 0);

    // Check each block reference in the record
    for (int block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
        Buffer buf;
        Page page;

        // Get block location information
        if (!XLogRecGetBlockTagExtended(record, block_id, &rlocator, &forknum, &blkno, NULL))
            continue;  // Skip if block reference doesn't exist

        Assert(XLogRecHasBlockImage(record, block_id));

        // Skip if page was already applied (would compare page with itself)
        if (XLogRecBlockImageApply(record, block_id))
            continue;

        // Read current page from buffer
        buf = XLogReadBufferExtended(rlocator, forknum, blkno, RBM_NORMAL_NO_LOG, InvalidBuffer);
        if (!BufferIsValid(buf))
            continue;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);

        // Copy current page for comparison
        memcpy(replay_image_masked, page, BLCKSZ);
        UnlockReleaseBuffer(buf);

        // Skip if page LSN is ahead of this record (recovery restart)
        if (PageGetLSN(replay_image_masked) > record->EndRecPtr)
            continue;

        // Restore backup image from WAL record
        if (!RestoreBlockImage(record, block_id, primary_image_masked))
            ereport(ERROR, "failed to restore backup image");

        // Apply masking if available (ignore hint bits, unused space, etc.)
        if (rmgr.rm_mask != NULL) {
            rmgr.rm_mask(replay_image_masked, blkno);
            rmgr.rm_mask(primary_image_masked, blkno);
        }

        // Compare the two page images
        if (memcmp(replay_image_masked, primary_image_masked, BLCKSZ) != 0) {
            elog(FATAL, "inconsistent page found, rel %u/%u/%u, forknum %u, blkno %u",
                 rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, forknum, blkno);
        }
    }
}
```