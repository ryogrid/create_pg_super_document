# fill_seq_fork_with_data

## Location
[src/backend/commands/sequence.c:359-436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L359-L436)

## Overview
fill_seq_fork_with_data initializes a specific fork of a sequence relation with tuple data, handling page initialization, tuple insertion, and WAL logging.

## Definition

```c
static void
fill_seq_fork_with_data(Relation rel, HeapTuple tuple, ForkNumber forkNum)
```
## Detailed Description
fill_seq_fork_with_data is a low-level function that performs the actual work of writing sequence data to a specific storage fork. It handles the complete process of sequence page initialization and data insertion with proper WAL logging.

The function performs these operations:
1. **Page Allocation**: Uses  to allocate the first page (block 0) of the specified fork
2. **Page Initialization**: Initializes the page with  and sets up sequence-specific magic number (SEQ_MAGIC)
3. **Tuple Preparation**: Modifies tuple headers for proper transaction visibility:
   - Sets xmin to FrozenTransactionId (sequences don't use VACUUM)
   - Sets tuple as frozen to prevent visibility issues after 2G transactions
   - Sets command ID and invalidates xmax
   - Updates tuple's ctid pointer
4. **Transaction Management**: Gets top transaction ID if WAL is needed
5. **Critical Section**: Protects the insertion operation:
   - Marks buffer as dirty
   - Adds tuple to page using 
   - Validates insertion succeeded
6. **WAL Logging**: For WAL-enabled relations or init forks:
   - Constructs xl_seq_rec WAL record
   - Registers buffer and tuple data
   - Inserts WAL record with RM_SEQ_ID/XLOG_SEQ_LOG
   - Sets page LSN

## Parameters / Member Variables
- : Relation representing the sequence
- : HeapTuple containing sequence data (last_value, log_cnt, is_called)
- : Fork number to write to (MAIN_FORKNUM or INIT_FORKNUM)

## Dependencies
- Functions called/Symbols referenced:
  - [ExtendBufferedRel](../E/ExtendBufferedRel.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageInit](../P/PageInit.md)
  - [PageGetSpecialPointer](../P/PageGetSpecialPointer.md)
  - HeapTupleHeaderSetXmin
  - HeapTupleHeaderSetXminFrozen
  - HeapTupleHeaderSetCmin
  - HeapTupleHeaderSetXmax
  - [GetTopTransactionId](../G/GetTopTransactionId.md)
  - PageAddItem
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from (representative examples):
  - [fill_seq_with_data](fill_seq_with_data.md) (src/backend/commands/sequence.c:340, 349)

## Notes and Other Information
- This is a static function used internally within sequence.c
- Sequences use a special page format with sequence_magic in the special space
- Tuple headers are specially crafted since sequences bypass normal VACUUM processing
- The function always writes to block 0 (sequences are single-page relations)
- Critical section ensures atomicity of buffer modifications and WAL logging
- Init fork operations are always WAL-logged even for unlogged sequences
- Proper error handling ensures sequence creation fails cleanly if page operations fail

## Simplified Source

```c
static void fill_seq_fork_with_data(Relation rel, HeapTuple tuple, ForkNumber forkNum) {
    Buffer buf;
    Page page;
    sequence_magic *sm;
    OffsetNumber offnum;

    // Extend relation to create first page (block 0)
    buf = ExtendBufferedRel(BMR_REL(rel), forkNum, NULL,
                           EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);
    page = BufferGetPage(buf);

    // Initialize page with sequence magic number in special space
    PageInit(page, BufferGetPageSize(buf), sizeof(sequence_magic));
    sm = (sequence_magic *) PageGetSpecialPointer(page);
    sm->magic = SEQ_MAGIC;

    // Prepare tuple for insertion: sequences bypass normal VACUUM
    // so we freeze the tuple to prevent visibility issues
    HeapTupleHeaderSetXmin(tuple->t_data, FrozenTransactionId);
    HeapTupleHeaderSetXminFrozen(tuple->t_data);
    HeapTupleHeaderSetCmin(tuple->t_data, FirstCommandId);
    HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
    tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
    ItemPointerSet(&tuple->t_data->t_ctid, 0, FirstOffsetNumber);

    // Get transaction ID for WAL if needed
    if (RelationNeedsWAL(rel))
        GetTopTransactionId();

    START_CRIT_SECTION();

    // Insert tuple into page
    MarkBufferDirty(buf);
    offnum = PageAddItem(page, (Item) tuple->t_data, tuple->t_len,
                        InvalidOffsetNumber, false, false);
    if (offnum != FirstOffsetNumber)
        elog(ERROR, "failed to add sequence tuple to page");

    // WAL logging for durability
    if (RelationNeedsWAL(rel) || forkNum == INIT_FORKNUM) {
        xl_seq_rec xlrec;
        XLogRecPtr recptr;

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
        xlrec.locator = rel->rd_locator;
        XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
        XLogRegisterData((char *) tuple->t_data, tuple->t_len);
        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();
    UnlockReleaseBuffer(buf);
}
```