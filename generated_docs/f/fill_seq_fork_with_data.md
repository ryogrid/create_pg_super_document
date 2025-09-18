# fill_seq_fork_with_data

## Location
src/backend/commands/sequence.c: 359 - 436

## Overview
fill_seq_fork_with_data initializes a specific fork of a sequence relation with tuple data, handling page initialization, tuple insertion, and WAL logging.

## Definition


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
  - PageInit
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