# logicalrep_write_attrs

## Location
[src/backend/replication/logical/proto.c:926-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L926-L992)

## Overview
Serializes PostgreSQL relation attribute metadata to the logical replication output stream, including column information and replica identity markers for selected columns.

## Definition
static void logicalrep_write_attrs(StringInfo out, Relation rel, Bitmapset *columns)

## Detailed Description
This function writes detailed attribute (column) metadata for a relation to the logical replication stream as part of relation definition messages. It processes each column in the relation, filtering out dropped and generated columns, and only including columns specified in the provided bitmapset.

The function performs two main phases:
1. Counts the number of live attributes to write the header
2. Iterates through all attributes, writing detailed information for each:
   - Flags indicating whether the column is part of the replica identity
   - Column name
   - Data type OID  
   - Type modifier information

The replica identity handling is particularly important - it determines which columns are considered part of the table's logical key for replication purposes. When REPLICA IDENTITY FULL is set, all columns are marked as key columns; otherwise, only explicitly defined identity key columns are marked.

## Parameters / Member Variables
- `out`: StringInfo buffer where the attribute metadata will be written
- `rel`: Relation object providing metadata about the table and its columns  
- `columns`: Bitmapset specifying which columns to include in the output

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr: Gets the tuple descriptor for the relation
  - TupleDescAttr: Accesses individual attribute information
  - [column_in_column_list](../c/column_in_column_list.md): Checks if a column should be included
  - [RelationGetIdentityKeyBitmap](../R/RelationGetIdentityKeyBitmap.md): Gets bitmap of replica identity key columns
  - [pq_sendint16](../p/pq_sendint16.md): Writes count of live attributes
  - [pq_sendbyte](../p/pq_sendbyte.md): Writes flags for each attribute
  - [pq_sendstring](../p/pq_sendstring.md): Writes attribute names
  - [pq_sendint32](../p/pq_sendint32.md): Writes type OID and type modifier
  - [bms_is_member](../b/bms_is_member.md): Checks if attribute is in identity key bitmap
  - [bms_free](../b/bms_free.md): Releases memory for identity key bitmap
  - REPLICA_IDENTITY_FULL: Constant for full replica identity mode
  - LOGICALREP_IS_REPLICA_IDENTITY: Flag constant for identity columns

- Called from (representative examples):
  - [logicalrep_write_rel](logicalrep_write_rel.md): When writing complete relation definitions

## Notes and Other Information
- Static function, only accessible within the logical replication protocol module
- Handles replica identity logic to mark key columns appropriately for conflict resolution
- Filters out dropped and generated columns to send only relevant attribute information
- Part of the relation definition protocol in logical replication
- Essential for subscribers to understand the structure and key columns of replicated tables
- Memory management includes proper cleanup of identity key bitmaps
- Supports partial column replication through the columns bitmapset parameter
- Critical for maintaining data consistency and enabling proper conflict resolution on subscribers

## Simplified Source

```c
static void logicalrep_write_attrs(StringInfo out, Relation rel, Bitmapset *columns)
{
    TupleDesc desc = RelationGetDescr(rel);
    uint16 nliveatts = 0;
    Bitmapset *idattrs = NULL;
    bool replidentfull;

    // Count live attributes to include
    for (int i = 0; i < desc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(desc, i);

        if (att->attisdropped || att->attgenerated)
            continue;
        if (!column_in_column_list(att->attnum, columns))
            continue;

        nliveatts++;
    }
    pq_sendint16(out, nliveatts);

    // Get replica identity information
    replidentfull = (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL);
    if (!replidentfull)
        idattrs = RelationGetIdentityKeyBitmap(rel);

    // Write each attribute's metadata
    for (int i = 0; i < desc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(desc, i);
        uint8 flags = 0;

        if (att->attisdropped || att->attgenerated)
            continue;
        if (!column_in_column_list(att->attnum, columns))
            continue;

        // Mark replica identity columns
        if (replidentfull ||
            bms_is_member(att->attnum - FirstLowInvalidHeapAttributeNumber, idattrs))
            flags |= LOGICALREP_IS_REPLICA_IDENTITY;

        // Send attribute metadata
        pq_sendbyte(out, flags);
        pq_sendstring(out, NameStr(att->attname));
        pq_sendint32(out, (int) att->atttypid);
        pq_sendint32(out, att->atttypmod);
    }

    bms_free(idattrs);
}
```