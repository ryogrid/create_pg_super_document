# logicalrep_write_typ

## Location
[src/backend/replication/logical/proto.c:725-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L725-L755)

## Overview
Writes PostgreSQL type information to the logical replication output stream, always writing base type information regardless of the input type (e.g., domain types are resolved to their base types).

## Definition

```c
void
logicalrep_write_typ(StringInfo out, TransactionId xid, Oid typoid)
```
## Detailed Description
This function serializes type metadata for logical replication by writing a LOGICAL_REP_MSG_TYPE message to the output stream. It resolves any complex types (like domains) to their base types using , then retrieves the type information from the system catalog (pg_type). The function outputs the type's OID, namespace, and type name in a format suitable for logical replication consumers.

The function is part of PostgreSQL's logical replication protocol implementation, ensuring that type information is correctly transmitted to subscribers so they can properly interpret data values.

## Parameters / Member Variables
- : StringInfo buffer where the serialized type information will be written
- : Transaction ID for streaming replication context (written to output if valid)
- : Object ID of the PostgreSQL type to serialize

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md): Resolves complex types to their base types
  - [SearchSysCache1](../S/SearchSysCache1.md): Looks up type information in the system cache
  - [pq_sendbyte](../p/pq_sendbyte.md): Writes the message type identifier
  - [pq_sendint32](../p/pq_sendint32.md): Writes 32-bit integers (transaction ID and type OID)
  - [pq_sendstring](../p/pq_sendstring.md): Writes null-terminated strings (type name)
  - [logicalrep_write_namespace](logicalrep_write_namespace.md): Writes namespace information
  - Form_pg_type: Structure representing pg_type catalog entries
  - LOGICAL_REP_MSG_TYPE: Message type constant for type information

- Called from (representative examples):
  - [send_relation_and_attrs](../s/send_relation_and_attrs.md) (in pgoutput plugin)

## Notes and Other Information
- Always operates on base types, automatically resolving domains and other complex types
- Includes transaction ID in the output when streaming replication is active
- Uses the system cache for efficient type lookup
- Part of the logical replication protocol specification
- The output format is consumed by logical replication subscribers to reconstruct type information