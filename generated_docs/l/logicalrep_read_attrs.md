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
- `in`: StringInfo buffer containing the incoming logical replication message data
- `*rel`: Pointer to LogicalRepRelation structure to be populated with attribute metadata
## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md) (extract integer values from message)
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md) (extract byte values from message)  
  - [pq_getmsgstring](../p/pq_getmsgstring.md) (extract string values from message)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - [bms_add_member](../b/bms_add_member.md) (add member to bitmap set)
  - LOGICALREP_IS_REPLICA_IDENTITY (flag constant)
- Called from:
  - [logicalrep_read_rel](logicalrep_read_rel.md) (reads complete relation information)

## Notes and Other Information
- This is a static function used internally within the logical replication protocol implementation
- The function allocates memory using palloc, which is automatically freed when the current memory context is destroyed
- Attribute mode information is read from the stream but currently ignored by the implementation
- The replica identity bitmap efficiently tracks which attributes are part of the table's replica identity
- Part of PostgreSQL's logical replication subsystem for streaming table schema information

## Simplified Source

```c
static void logicalrep_read_attrs(StringInfo in, LogicalRepRelation *rel)
{
    // Read number of attributes
    int natts = pq_getmsgint(in, 2);

    // Allocate arrays for attribute metadata
    char **attnames = palloc(natts * sizeof(char *));
    Oid *atttyps = palloc(natts * sizeof(Oid));
    Bitmapset *attkeys = NULL;

    // Read each attribute's metadata
    for (int i = 0; i < natts; i++)
    {
        // Check if attribute is part of replica identity
        uint8 flags = pq_getmsgbyte(in);
        if (flags & LOGICALREP_IS_REPLICA_IDENTITY)
            attkeys = bms_add_member(attkeys, i);

        // Read attribute name and type
        attnames[i] = pstrdup(pq_getmsgstring(in));
        atttyps[i] = (Oid) pq_getmsgint(in, 4);

        // Skip attribute mode (not currently used)
        (void) pq_getmsgint(in, 4);
    }

    // Store results in relation structure
    rel->attnames = attnames;
    rel->atttyps = atttyps;
    rel->attkeys = attkeys;
    rel->natts = natts;
}
```