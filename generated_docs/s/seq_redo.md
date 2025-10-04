# seq_redo

## Location
[src/backend/commands/sequence.c:1834-1886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L1834-L1886)

## Overview
Handles WAL (Write-Ahead Log) redo operations for sequence-related log records during crash recovery and replication.

## Definition

```c
void
seq_redo(XLogReaderState *record)
```
## Detailed Description
The  function is a critical component of PostgreSQL's crash recovery system, specifically handling the replay of sequence-related WAL records. During database recovery or on standby servers, this function reconstructs sequence pages from logged information to maintain data consistency.

The function performs several important operations:
1. **WAL record validation**: Verifies the record type is XLOG_SEQ_LOG
2. **Buffer initialization**: Prepares the target buffer for redo operations
3. **Page reconstruction**: Builds a new page with proper sequence metadata
4. **Hot-standby safety**: Uses a local workspace to prevent transient corruption that could affect concurrent readers
5. **Data restoration**: Adds the logged sequence data item to the reconstructed page

A key design feature is the use of a local page buffer to avoid transiently corrupting the shared buffer during reconstruction, which is essential for hot-standby scenarios where other backends might be concurrently reading the sequence.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record data, LSN information, and other metadata needed for redo processing
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecGetData
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetPageSize](../B/BufferGetPageSize.md)
  - [palloc](../p/palloc.md)
  - [PageInit](../P/PageInit.md)
  - [PageGetSpecialPointer](../P/PageGetSpecialPointer.md)
  - XLogRecGetDataLen
  - PageAddItem
  - [PageSetLSN](../P/PageSetLSN.md)
  - memcpy
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [pfree](../p/pfree.md)
- Called from:
  - No direct references found (likely registered as a WAL redo handler)

## Notes and Other Information
- This function is part of PostgreSQL's WAL system and is called automatically during recovery operations
- The hot-standby safety mechanism using local page construction is a sophisticated approach to maintaining consistency
- Error handling includes PANIC-level errors for unexpected record types or page corruption
- Memory management includes proper allocation and deallocation of the local workspace
- The sequence magic number (SEQ_MAGIC) is used to validate sequence page integrity
- Located in src/backend/commands/sequence.c:1834-1886

## Simplified Source

```c
void seq_redo(XLogReaderState *record) {
    XLogRecPtr lsn = record->EndRecPtr;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    Buffer buffer;
    Page page;
    Page localpage;
    char *item;
    Size itemsz;
    xl_seq_rec *xlrec = (xl_seq_rec *) XLogRecGetData(record);
    sequence_magic *sm;

    // Validate WAL record type
    if (info != XLOG_SEQ_LOG)
        elog(PANIC, "seq_redo: unknown op code %u", info);

    // Initialize buffer for redo operation
    buffer = XLogInitBufferForRedo(record, 0);
    page = (Page) BufferGetPage(buffer);

    // Build new page in local workspace to avoid transient corruption
    // during hot-standby operations where other backends might read concurrently
    localpage = (Page) palloc(BufferGetPageSize(buffer));

    // Initialize new sequence page with magic number
    PageInit(localpage, BufferGetPageSize(buffer), sizeof(sequence_magic));
    sm = (sequence_magic *) PageGetSpecialPointer(localpage);
    sm->magic = SEQ_MAGIC;

    // Extract sequence data from WAL record
    item = (char *) xlrec + sizeof(xl_seq_rec);
    itemsz = XLogRecGetDataLen(record) - sizeof(xl_seq_rec);

    // Add sequence data item to the new page
    if (PageAddItem(localpage, (Item) item, itemsz,
                    FirstOffsetNumber, false, false) == InvalidOffsetNumber)
        elog(PANIC, "seq_redo: failed to add item to page");

    // Set LSN and copy to shared buffer
    PageSetLSN(localpage, lsn);
    memcpy(page, localpage, BufferGetPageSize(buffer));

    // Mark buffer dirty and release
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);
    pfree(localpage);
}
```