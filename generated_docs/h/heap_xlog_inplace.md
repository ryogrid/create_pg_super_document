# heap_xlog_inplace

## Location
[src/backend/access/heap/heapam.c:10297-10337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L10297-L10337)

## Overview
Handles the replay of in-place tuple updates during WAL recovery by directly replacing tuple data without changing the tuple's location or creating new tuple versions.

## Definition
```c
static void heap_xlog_inplace(XLogReaderState *record)
```

## Detailed Description
The `heap_xlog_inplace` function processes in-place tuple update operations during PostgreSQL's WAL recovery. This function is specifically designed for a special class of updates where the tuple data can be modified directly without creating a new tuple version or changing the tuple's physical location on the page.

In-place updates are a PostgreSQL optimization used in specific scenarios where:
1. **No MVCC concerns**: The update doesn't affect transaction visibility (typically system catalog updates)
2. **Same-size data**: The new tuple data has exactly the same length as the original data
3. **No concurrency issues**: The update can be performed atomically without affecting other transactions

Key characteristics of in-place updates:
1. **Direct data replacement**: The tuple's data portion is directly overwritten with new data
2. **Length validation**: Ensures the old and new data have identical lengths to maintain page structure
3. **Header preservation**: The tuple header remains unchanged, only the data portion is modified
4. **No versioning**: Unlike regular updates, no new tuple version is created

This operation is primarily used for system catalog maintenance and other specialized scenarios where MVCC semantics are not required.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with in-place update information, including the target tuple offset and the new tuple data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extracts xl_heap_inplace structure from WAL record)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads and locks target buffer for redo operations)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md) (retrieves the new tuple data from the WAL record)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md), PageGetItemId, PageGetItem (page-level tuple access functions)
  - ItemIdGetLength (retrieves the current tuple length for validation)
  - memcpy (performs the actual data replacement)
  - [PageSetLSN](../P/PageSetLSN.md), MarkBufferDirty (page maintenance operations)
- Called from (representative examples):
  - [heap_redo](heap_redo.md) (main heap WAL replay dispatcher)

## Notes and Other Information
- **Length Validation**: Critical safety check ensures old and new tuple data have identical lengths to prevent page corruption
- **Data-Only Updates**: Only the tuple's data portion (after t_hoff) is modified; the header remains unchanged
- **System Usage**: Primarily used for system catalog updates and other scenarios where MVCC semantics are not required
- **Atomic Operation**: The entire data replacement is performed as a single atomic memcpy operation
- **No Transaction Effects**: Since this doesn't create new tuple versions, it doesn't affect transaction visibility or MVCC semantics
- **Error Handling**: Includes PANIC-level validation for both tuple existence and length consistency
- **Recovery Simplicity**: One of the simpler WAL replay operations due to its straightforward data replacement nature

## Simplified Source

```c
static void heap_xlog_inplace(XLogReaderState *record) {
    xl_heap_inplace *xlrec = (xl_heap_inplace *) XLogRecGetData(record);
    Buffer buffer;

    // Read the target buffer for redo operation
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        // Get new tuple data from WAL record
        Size newlen;
        char *newtup = XLogRecGetBlockData(record, 0, &newlen);

        Page page = BufferGetPage(buffer);
        OffsetNumber offnum = xlrec->offnum;

        // Locate the target tuple
        ItemId lp = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(lp)) {
            elog(PANIC, "invalid lp");
        }

        HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, lp);

        // Validate tuple length matches exactly
        uint32 oldlen = ItemIdGetLength(lp) - htup->t_hoff;
        if (oldlen != newlen) {
            elog(PANIC, "wrong tuple length");
        }

        // Replace tuple data in-place (preserving header)
        memcpy((char *) htup + htup->t_hoff, newtup, newlen);

        // Mark page as modified
        PageSetLSN(page, record->EndRecPtr);
        MarkBufferDirty(buffer);
    }

    // Release buffer if valid
    if (BufferIsValid(buffer)) {
        UnlockReleaseBuffer(buffer);
    }
}
```