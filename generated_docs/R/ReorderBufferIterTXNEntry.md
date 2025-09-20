# ReorderBufferIterTXNEntry

## Location
[src/backend/replication/logical/reorderbuffer.c:158-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L158-L165)

## Overview
ReorderBufferIterTXNEntry is a structure that supports k-way in-order iteration of transaction changes, containing all necessary information to track and iterate through changes from a specific transaction.

## Definition

```c
typedef struct ReorderBufferIterTXNEntry
{
	XLogRecPtr	lsn;
	ReorderBufferChange *change;
	ReorderBufferTXN *txn;
	TXNEntryFile file;
	XLogSegNo	segno;
} ReorderBufferIterTXNEntry;
```
## Detailed Description
This structure is a key component of PostgreSQL's logical replication system that enables k-way merging of changes from multiple transactions in LSN order. When decoding WAL records for logical replication, changes from different transactions need to be processed in the correct chronological order based on their Log Sequence Numbers (LSN). This structure maintains the state for one transaction's stream of changes during the merge process, including the current change being processed, the transaction context, file information for spilled changes, and WAL segment information. The k-way merge algorithm uses multiple instances of this structure to efficiently combine changes from different transactions while maintaining proper ordering.

## Parameters / Member Variables
- `lsn`: XLogRecPtr containing the Log Sequence Number of the current change, used for ordering changes across transactions
- `*change`: Pointer to the current ReorderBufferChange being processed from this transaction stream
- `*txn`: Pointer to the ReorderBufferTXN structure representing the transaction this entry corresponds to
- `file`: TXNEntryFile structure managing the file descriptor and offset for reading spilled transaction changes from disk
- `segno`: XLogSegNo representing the WAL segment number associated with the current change
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (PostgreSQL WAL Log Sequence Number type)
  - [ReorderBufferChange](ReorderBufferChange.md) (change record structure)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (transaction state structure)
  - [TXNEntryFile](../T/TXNEntryFile.md) (file management structure)
  - XLogSegNo (WAL segment number type)
- Called from (representative examples):
  - ReorderBufferIterTXNState (at src/backend/replication/logical/reorderbuffer.c:172)
  - [ReorderBufferIterTXNInit](ReorderBufferIterTXNInit.md) (at src/backend/replication/logical/reorderbuffer.c:1318)
  - [ReorderBufferIterTXNNext](ReorderBufferIterTXNNext.md) (at src/backend/replication/logical/reorderbuffer.c:1411)

## Notes and Other Information
This structure is essential for the k-way merge algorithm used in logical replication decoding. Multiple ReorderBufferIterTXNEntry structures are maintained simultaneously, each representing a different transaction's change stream. The merge algorithm uses the LSN values to determine the correct order for processing changes across all transactions. The file component is particularly important when transactions have been spilled to disk due to memory constraints, allowing the iterator to seamlessly read changes from persistent storage when needed.