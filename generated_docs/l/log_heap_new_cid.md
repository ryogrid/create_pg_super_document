# log_heap_new_cid

## Location
[src/backend/access/heap/heapam.c:9038-9118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L9038-L9118)

## Overview
Performs XLogInsert of an XLOG_HEAP2_NEW_CID record for tracking command IDs (CIDs) of catalog tuples in logical decoding contexts.

## Definition


## Detailed Description
The  function creates WAL records specifically for tracking command IDs of catalog tuples when wal_level is set to logical or higher. This is essential for logical decoding to properly reconstruct the sequence of operations within a transaction. The function extracts and logs the creation command ID (cmin) and/or maximum command ID (cmax) from tuple headers, handling both combo CID scenarios (where a tuple is inserted and deleted within the same transaction) and regular cases.

The function differentiates between tuples that have combo CIDs (inserted and deleted in the same transaction) versus those with simple command IDs. It also handles special cases like lock-only operations where xmax is set but the tuple is not actually deleted.

## Parameters / Member Variables
- : The catalog relation containing the tuple
- : The heap tuple whose command ID information needs to be logged

## Dependencies
- Functions called/Symbols referenced:
  - [xl_heap_new_cid](../x/xl_heap_new_cid.md) (WAL record structure)
  - [GetTopTransactionId](../G/GetTopTransactionId.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - HeapTupleHeaderGetCmin
  - HeapTupleHeaderGetCmax
  - HeapTupleHeaderGetRawCommandId
  - HeapTupleHeaderXminInvalid
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - HEAP_COMBOCID
  - HEAP_XMAX_INVALID
  - HEAP_XMAX_IS_LOCKED_ONLY
  - InvalidCommandId
  - XLOG_HEAP2_NEW_CID
- Called from:
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- This function is static and only used internally within heapam.c
- Only active when wal_level >= WAL_LEVEL_LOGICAL and only for catalog tuples
- Does not register any buffers since it doesn't modify pages - the actual insert/update/delete operations handle page modifications separately
- Essential for logical replication and logical decoding to maintain transaction semantics
- Handles combo CID scenarios where tuples are both inserted and deleted within the same transaction
- Distinguishes between actual deletions and lock-only operations (FOR KEY SHARE updates)
- The function asserts that the tuple has a valid TID and table OID
- Returns an XLogRecPtr representing the LSN of the inserted WAL record
- WAL records are examined regardless of origin for logical decoding purposes