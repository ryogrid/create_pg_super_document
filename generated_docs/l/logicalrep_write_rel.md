# logicalrep_write_rel

## Location
[src/backend/replication/logical/proto.c:670-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L670-L699)

## Overview
Serializes and writes a relation (table) description to the logical replication output stream, transmitting schema information needed by subscribers.

## Definition
```c
void logicalrep_write_rel(StringInfo out, TransactionId xid, Relation rel,
                         Bitmapset *columns)
```

## Detailed Description
This function encodes a relation description into the logical replication protocol format. It writes the relation message type, transaction ID (if valid), relation OID, qualified relation name (including namespace), replica identity setting, and attribute information for the specified columns. This allows subscribers to understand the schema of tables involved in replication operations and properly apply changes.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized relation description will be written
- `xid`: Transaction ID associated with the relation message (only sent if valid)
- `rel`: Relation structure representing the table whose schema is being transmitted
- `columns`: Bitmapset indicating which columns should be included in the attribute description

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendstring](../p/pq_sendstring.md)
  - logicalrep_write_namespace
  - [logicalrep_write_attrs](logicalrep_write_attrs.md)
  - RelationGetRelid
  - RelationGetNamespace
  - RelationGetRelationName
  - LOGICAL_REP_MSG_RELATION
- Called from (representative examples):
  - [send_relation_and_attrs](../s/send_relation_and_attrs.md)

## Notes and Other Information
- Part of the logical replication protocol for schema transmission
- [Relation](../R/Relation.md) descriptions are sent before data changes to ensure subscribers have current schema information
- Uses relation OID as the primary identifier for efficient lookups
- Includes replica identity information crucial for UPDATE/DELETE operations
- The columns parameter allows selective transmission of column metadata
- Namespace information ensures proper schema qualification on the subscriber side
- Transaction ID is conditionally sent only when valid (for streaming transactions)