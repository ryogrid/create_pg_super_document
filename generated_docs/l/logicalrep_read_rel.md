# logicalrep_read_rel

## Location
src/backend/replication/logical/proto.c: 700 - 724

## Overview
Deserializes and reads a relation (table) description from the logical replication input stream, reconstructing schema information for subscriber use.

## Definition
```c
LogicalRepRelation *logicalrep_read_rel(StringInfo in)
```

## Detailed Description
This function is the counterpart to logicalrep_write_rel, responsible for parsing a relation description received through logical replication. It reads the serialized relation data from the input stream and constructs a LogicalRepRelation structure containing the remote relation ID, namespace name, relation name, replica identity setting, and attribute information. The function allocates memory for the relation structure and returns it to the caller.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the serialized relation description to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - palloc
  - pq_getmsgint
  - pstrdup
  - logicalrep_read_namespace
  - pq_getmsgstring
  - pq_getmsgbyte
  - logicalrep_read_attrs
  - LogicalRepRelation
- Called from (representative examples):
  - apply_handle_relation

## Notes and Other Information
- Part of the logical replication protocol infrastructure for schema deserialization
- Returns a dynamically allocated LogicalRepRelation structure that must be freed by the caller
- Complementary function to logicalrep_write_rel for protocol communication
- Uses PostgreSQL's pq_getmsg* family of functions for binary deserialization
- The remoteid field stores the OID of the relation on the publisher side
- Namespace and relation names are duplicated using pstrdup for independent memory management
- Used by logical replication workers to understand table schemas before applying data changes
- Critical for maintaining schema consistency between publisher and subscriber