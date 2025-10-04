# logicalrep_write_commit

## Location
[src/backend/replication/logical/proto.c:89-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L89-L108)

## Overview
Writes a COMMIT message to the logical replication output stream to indicate the successful completion of a transaction.

## Definition

```c
void
logicalrep_write_commit(StringInfo out, ReorderBufferTXN *txn,
						XLogRecPtr commit_lsn)
```
## Detailed Description
This function serializes a COMMIT message into the logical replication protocol stream, marking the end of a transaction. It includes transaction metadata such as the commit LSN, end LSN, and commit timestamp. The function follows the logical replication protocol specification and includes a flags field (currently unused but reserved for future extensions) along with the essential transaction completion information.

The COMMIT message serves as the counterpart to the BEGIN message and is essential for maintaining transaction boundaries in logical replication streams.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized COMMIT message will be written
- `*txn`: ReorderBufferTXN structure containing transaction information to be serialized
- `commit_lsn`: XLogRecPtr representing the LSN where the transaction was committed
## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md) (sends a single byte to the output buffer)
  - [pq_sendint64](../p/pq_sendint64.md) (sends a 64-bit integer to the output buffer)
  - LOGICAL_REP_MSG_COMMIT (message type constant for COMMIT messages)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md) (transaction structure type)
  - XLogRecPtr (LSN pointer type)
- Called from (representative examples):
  - [pgoutput_commit_txn](../p/pgoutput_commit_txn.md) (in the pgoutput plugin for sending COMMIT messages to subscribers)

## Notes and Other Information
- This function is part of the logical replication protocol implementation
- The flags field is currently unused but reserved for future protocol extensions
- The COMMIT message includes commit LSN, end LSN, and commit timestamp as fixed fields
- Used by logical replication output plugins to communicate transaction completion to subscribers
- The message format is standardized and must be compatible with logical replication subscribers
- Located in src/backend/replication/logical/proto.c as part of the protocol encoding functions
- Works in conjunction with logicalrep_write_begin to define transaction boundaries

## Simplified Source

```c
void logicalrep_write_commit(StringInfo out, ReorderBufferTXN *txn, XLogRecPtr commit_lsn) {
    // Send COMMIT message type
    pq_sendbyte(out, LOGICAL_REP_MSG_COMMIT);

    // Send flags field (unused for now)
    pq_sendbyte(out, 0);

    // Send transaction completion data
    pq_sendint64(out, commit_lsn);        // Commit LSN
    pq_sendint64(out, txn->end_lsn);      // Transaction end LSN
    pq_sendint64(out, txn->xact_time.commit_time);  // Commit timestamp
}
```