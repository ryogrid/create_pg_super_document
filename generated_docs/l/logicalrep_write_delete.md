# logicalrep_write_delete

## Location
[src/backend/replication/logical/proto.c:533-563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L533-L563)

## Overview
Writes a DELETE message to the logical replication output stream, encoding the old tuple data according to the logical replication protocol.

## Definition

```c
void
logicalrep_write_delete(StringInfo out, TransactionId xid, Relation rel,
						TupleTableSlot *oldslot, bool binary,
						Bitmapset *columns)
```
## Detailed Description
This function serializes a DELETE operation into the logical replication wire protocol format. It constructs the DELETE message by writing the message type, optional transaction ID, relation identifier, and the old tuple data. The function respects the table's replica identity setting to determine whether to include the full old tuple ('O') or just the key columns ('K').

The DELETE message format includes:
1. Message type identifier (LOGICAL_REP_MSG_DELETE)
2. Optional transaction ID (if streaming)
3. Relation OID to identify the target table
4. Tuple type marker ('O' for full tuple or 'K' for key-only)
5. Old tuple data in the specified format

## Parameters / Member Variables
- : StringInfo buffer where the DELETE message will be written
- : Transaction ID (optional, used when streaming transactions)
- : Relation descriptor for the table from which the row is being deleted
- : TupleTableSlot containing the old tuple values to be deleted
- : Boolean flag indicating whether to use binary or text format
- : Bitmapset specifying which columns to include in the output

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md) (sends single byte to output stream)
  - [pq_sendint32](../p/pq_sendint32.md) (sends 4-byte integer to output stream)
  - RelationGetRelid (gets relation OID)
  - TransactionIdIsValid (validates transaction ID)
  - [logicalrep_write_tuple](logicalrep_write_tuple.md) (writes tuple data to stream)
- Constants used:
  - LOGICAL_REP_MSG_DELETE (message type identifier)
  - REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL, REPLICA_IDENTITY_INDEX (replica identity modes)
- Called from (representative examples):
  - [pgoutput_change](../p/pgoutput_change.md) (in pgoutput logical decoding plugin)

## Notes and Other Information
- Asserts that the relation has a valid replica identity setting (DEFAULT, FULL, or INDEX)
- Uses 'O' marker for full old tuple when replica identity is FULL, 'K' for key-only otherwise
- Transaction ID is only written if valid (used in streaming replication scenarios)
- The function is part of PostgreSQL's logical replication protocol encoder
- Located in src/backend/replication/logical/proto.c:533-563