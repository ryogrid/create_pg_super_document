# logicalrep_write_insert

## Location
[src/backend/replication/logical/proto.c:414-435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L414-L435)

## Overview
This function writes an INSERT message to the logical replication output stream, serializing information about a newly inserted tuple for replication.

## Definition

```c
void
logicalrep_write_insert(StringInfo out, TransactionId xid, Relation rel,
						TupleTableSlot *newslot, bool binary, Bitmapset *columns)
```
## Detailed Description
The  function serializes an INSERT operation into the logical replication stream format. It creates a message with the  type, followed by optional transaction ID (for streaming transactions), the relation OID, and the actual tuple data. The function marks the tuple data with 'N' (indicating "new tuple") before delegating the actual tuple serialization to .

This function is a core component of PostgreSQL's logical replication system, responsible for transmitting INSERT operations to logical replication subscribers. It handles both streaming and non-streaming transaction contexts and supports binary format transmission when requested.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized INSERT message will be written
- `xid`: Transaction ID for streaming transactions (may be invalid for non-streaming contexts)
- `rel`: Relation object representing the table where the INSERT occurred
- `*newslot`: TupleTableSlot containing the inserted tuple data
- `binary`: Boolean flag indicating whether to use binary format for tuple transmission
- `*columns`: Bitmapset specifying which columns to include in the replication message
## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - LOGICAL_REP_MSG_INSERT
  - [pq_sendint32](../p/pq_sendint32.md)
  - RelationGetRelid
  - [logicalrep_write_tuple](logicalrep_write_tuple.md)
  - TransactionIdIsValid
- Called from (representative examples):
  - [pgoutput_change](../p/pgoutput_change.md)

## Notes and Other Information
- Transaction ID is only sent for streaming transactions (when TransactionIdIsValid returns true)
- Uses relation OID as the identifier for the affected table
- The 'N' marker indicates this is a "new tuple" (as opposed to old tuple in UPDATE/DELETE)
- Supports selective column replication through the columns bitmapset parameter
- Part of PostgreSQL's logical replication protocol implementation
- Located in src/backend/replication/logical/proto.c:414-435

## Simplified Source

```c
void logicalrep_write_insert(StringInfo out, TransactionId xid, Relation rel,
                            TupleTableSlot *newslot, bool binary, Bitmapset *columns) {
    // Write INSERT message type
    pq_sendbyte(out, LOGICAL_REP_MSG_INSERT);

    // Send transaction ID if streaming
    if (TransactionIdIsValid(xid))
        pq_sendint32(out, xid);

    // Send relation identifier and new tuple
    pq_sendint32(out, RelationGetRelid(rel));
    pq_sendbyte(out, 'N');  // Mark as new tuple
    logicalrep_write_tuple(out, rel, newslot, binary, columns);
}
```