# logicalrep_read_attrs

## Location
[src/backend/replication/logical/proto.c:993-1034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L993-L1034)

## Overview
Reads relation attribute metadata from a logical replication message stream and populates the LogicalRepRelation structure with attribute names, types, and replica identity information.

## Definition

```c
static void
logicalrep_read_attrs(StringInfo in, LogicalRepRelation *rel)
```
## Detailed Description
This function parses attribute metadata from a logical replication protocol message stream. It reads the number of attributes, then iterates through each attribute to extract:
- Flags indicating whether the attribute is part of the replica identity
- Attribute name as a string
- Attribute type OID
- Attribute mode (currently ignored)

The function allocates memory for attribute arrays and populates the LogicalRepRelation structure with the parsed information. Attributes marked with the LOGICALREP_IS_REPLICA_IDENTITY flag are added to a bitmap set for efficient tracking of replica identity columns.

## Parameters / Member Variables
- : StringInfo buffer containing the incoming logical replication message data
- : Pointer to LogicalRepRelation structure to be populated with attribute metadata

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint (extract integer values from message)
  - pq_getmsgbyte (extract byte values from message)  
  - pq_getmsgstring (extract string values from message)
  - palloc (memory allocation)
  - pstrdup (string duplication)
  - bms_add_member (add member to bitmap set)
  - LOGICALREP_IS_REPLICA_IDENTITY (flag constant)
- Called from:
  - logicalrep_read_rel (reads complete relation information)

## Notes and Other Information
- This is a static function used internally within the logical replication protocol implementation
- The function allocates memory using palloc, which is automatically freed when the current memory context is destroyed
- Attribute mode information is read from the stream but currently ignored by the implementation
- The replica identity bitmap efficiently tracks which attributes are part of the table's replica identity
- Part of PostgreSQL's logical replication subsystem for streaming table schema information