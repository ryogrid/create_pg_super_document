# LogicalRepBeginData

## Location
[src/include/replication/logicalproto.h:127-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/logicalproto.h#L127-L132)

## Overview
LogicalRepBeginData is a structure that contains transaction information for the beginning of a logical replication transaction, providing essential metadata needed to start transaction processing.

## Definition

```c
typedef struct LogicalRepBeginData
{
	XLogRecPtr	final_lsn;
	TimestampTz committime;
	TransactionId xid;
} LogicalRepBeginData;
```
## Detailed Description
This structure encapsulates the essential information needed to begin processing a transaction in logical replication. It serves as a transaction header that provides the subscriber with critical metadata about the transaction that is about to be replicated. The structure includes the transaction's final LSN (Log Sequence Number), commit timestamp, and transaction ID, which are all necessary for proper transaction ordering, conflict resolution, and maintaining consistency during replication.

The final_lsn field is particularly important as it represents the LSN where the transaction was committed on the publisher, enabling proper ordering and conflict detection. The commit timestamp helps with temporal ordering and debugging, while the transaction ID provides a unique identifier for the transaction.

## Parameters / Member Variables
- `final_lsn`: XLogRecPtr representing the LSN (Log Sequence Number) where this transaction was committed on the publisher side
- `committime`: TimestampTz indicating the timestamp when the transaction was committed on the publisher
- `xid`: TransactionId providing the unique identifier for this transaction
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (PostgreSQL log sequence number type)
  - TimestampTz (PostgreSQL timestamp with timezone type)
  - TransactionId (PostgreSQL transaction identifier type)
- Called from (representative examples):
  - [logicalrep_read_begin](../l/logicalrep_read_begin.md)
  - [apply_handle_begin](../a/apply_handle_begin.md)

## Notes and Other Information
- This structure marks the beginning of a transaction in the logical replication stream
- The final_lsn is crucial for maintaining proper transaction ordering and detecting conflicts
- The commit timestamp can be used for temporal ordering and debugging replication issues
- Transaction ID helps in tracking and identifying specific transactions across the replication process
- This structure is typically followed by the actual transaction data (inserts, updates, deletes)
- Located in src/include/replication/logicalproto.h:127-132