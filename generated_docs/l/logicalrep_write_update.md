# logicalrep_write_update

## Location
[src/backend/replication/logical/proto.c:458-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L458-L491)

## Overview
Writes an UPDATE message to the logical replication output stream, encoding both old and new tuple data according to the logical replication protocol.

## Definition

```c
void
logicalrep_write_update(StringInfo out, TransactionId xid, Relation rel,
						TupleTableSlot *oldslot, TupleTableSlot *newslot,
						bool binary, Bitmapset *columns)
```
## Detailed Description
This function serializes an UPDATE operation into the logical replication wire protocol format. It handles different replica identity modes (DEFAULT, FULL, INDEX) and writes the appropriate tuple data for both old and new values. The function constructs the UPDATE message by writing:

1. Message type identifier (LOGICAL_REP_MSG_UPDATE)
2. Optional transaction ID (if streaming)
3. Relation OID to identify the target table
4. Old tuple data (marked as 'O' for full tuple or 'K' for key-only based on replica identity)
5. New tuple data (marked as 'N')

The function respects the table's replica identity setting to determine what old tuple information to include in the stream.

## Parameters / Member Variables
- `out`: StringInfo buffer where the UPDATE message will be written
- `xid`: Transaction ID (optional, used when streaming transactions)
- `rel`: Relation descriptor for the table being updated
- `*oldslot`: TupleTableSlot containing the old tuple values (can be NULL)
- `*newslot`: TupleTableSlot containing the new tuple values
- `binary`: Boolean flag indicating whether to use binary or text format
- `*columns`: Bitmapset specifying which columns to include in the output
## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md) (sends single byte to output stream)
  - [pq_sendint32](../p/pq_sendint32.md) (sends 4-byte integer to output stream)  
  - RelationGetRelid (gets relation OID)
  - TransactionIdIsValid (validates transaction ID)
  - [logicalrep_write_tuple](logicalrep_write_tuple.md) (writes tuple data to stream)
- Constants used:
  - LOGICAL_REP_MSG_UPDATE (message type identifier)
  - REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL, REPLICA_IDENTITY_INDEX (replica identity modes)
- Called from (representative examples):
  - [pgoutput_change](../p/pgoutput_change.md) (in pgoutput logical decoding plugin)

## Notes and Other Information
- Asserts that the relation has a valid replica identity setting (DEFAULT, FULL, or INDEX)
- The old tuple marker depends on replica identity: 'O' for full tuple, 'K' for key-only
- Transaction ID is only written if valid (used in streaming replication scenarios)
- The function is part of PostgreSQL's logical replication protocol encoder
- Located in src/backend/replication/logical/proto.c:458-491

## Simplified Source

```c
void logicalrep_write_update(StringInfo out, TransactionId xid, Relation rel,
                            TupleTableSlot *oldslot, TupleTableSlot *newslot,
                            bool binary, Bitmapset *columns) {
    // Write UPDATE message type
    pq_sendbyte(out, LOGICAL_REP_MSG_UPDATE);

    // Validate replica identity setting
    Assert(rel->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT ||
           rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL ||
           rel->rd_rel->relreplident == REPLICA_IDENTITY_INDEX);

    // Send transaction ID if streaming
    if (TransactionIdIsValid(xid))
        pq_sendint32(out, xid);

    // Send relation identifier
    pq_sendint32(out, RelationGetRelid(rel));

    // Send old tuple if available
    if (oldslot != NULL) {
        if (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
            pq_sendbyte(out, 'O');  // Full old tuple
        else
            pq_sendbyte(out, 'K');  // Key-only old tuple
        logicalrep_write_tuple(out, rel, oldslot, binary, columns);
    }

    // Send new tuple
    pq_sendbyte(out, 'N');
    logicalrep_write_tuple(out, rel, newslot, binary, columns);
}
```