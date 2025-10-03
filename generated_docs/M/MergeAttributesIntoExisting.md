# MergeAttributesIntoExisting

## Location
[src/backend/commands/tablecmds.c:15896-16015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15896-L16015)

## Overview
MergeAttributesIntoExisting is a static function that validates attribute compatibility between child and parent relations during inheritance creation, and increments the inheritance count for matching attributes.

## Definition

```c
static void
MergeAttributesIntoExisting(Relation child_rel, Relation parent_rel, bool ispartition)
```
## Detailed Description
This function performs comprehensive attribute validation when establishing inheritance relationships between tables. It ensures that all parent columns exist in the child table with compatible properties, then updates the inheritance counts accordingly. The function enforces strict compatibility requirements: data types must match exactly, collations must be identical, NOT NULL constraints must be preserved, and generated column status must be consistent.

For each non-dropped attribute in the parent relation, the function:
1. Searches for a matching column by name in the child relation
2. Validates data type compatibility (type OID and type modifier)
3. Checks collation compatibility
4. Ensures NOT NULL constraints are preserved (child cannot be nullable if parent is NOT NULL)
5. Validates generated column consistency
6. For partitions, handles identity column inheritance and sets attislocal appropriately
7. Increments the attribute's inheritance count (attinhcount)
8. Updates the catalog with the modified attribute information

## Parameters / Member Variables
- `child_rel`: The child relation being established as an inheritor
- `parent_rel`: The parent relation to inherit attributes from
- `ispartition`: Boolean flag indicating if this is a partition relationship (affects identity column and attislocal handling)
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - RelationGetDescr
  - TupleDescAttr
  - NameStr
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - ereport
  - RelationGetRelationName
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [CreateInheritance](../C/CreateInheritance.md)
  - child_dependency_type

## Notes and Other Information
- Currently requires all parent columns to exist in child - missing columns result in an error
- Does not compare default values between parent and child columns
- For partitions, inherits identity column properties and sets attislocal to false
- Prevents inheritance count overflow by checking for negative values after increment
- Uses RowExclusiveLock on pg_attribute catalog for safe concurrent access
- Future consideration mentioned for auto-creating missing columns like CREATE TABLE, but currently rejected as a 'foot-gun' for partitioned tables
- All attribute modifications are transactional and will rollback if the operation fails later

## Simplified Source

```c
static void
MergeAttributesIntoExisting(Relation child_rel, Relation parent_rel, bool ispartition)
{
    Relation attrrel;
    TupleDesc parent_desc;

    attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    parent_desc = RelationGetDescr(parent_rel);

    // Check each parent attribute exists in child with matching properties
    for (AttrNumber parent_attno = 1; parent_attno <= parent_desc->natts; parent_attno++)
    {
        Form_pg_attribute parent_att = TupleDescAttr(parent_desc, parent_attno - 1);
        char *parent_attname = NameStr(parent_att->attname);
        HeapTuple tuple;

        // Skip dropped columns
        if (parent_att->attisdropped)
            continue;

        // Find matching column in child
        tuple = SearchSysCacheCopyAttName(RelationGetRelid(child_rel), parent_attname);
        if (HeapTupleIsValid(tuple))
        {
            Form_pg_attribute child_att = (Form_pg_attribute) GETSTRUCT(tuple);

            // Validate type compatibility
            if (parent_att->atttypid != child_att->atttypid ||
                parent_att->atttypmod != child_att->atttypmod)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                               errmsg("child table \"%s\" has different type for column \"%s\"",
                                      RelationGetRelationName(child_rel), parent_attname)));

            // Validate collation compatibility
            if (parent_att->attcollation != child_att->attcollation)
                ereport(ERROR, (errcode(ERRCODE_COLLATION_MISMATCH),
                               errmsg("child table \"%s\" has different collation for column \"%s\"",
                                      RelationGetRelationName(child_rel), parent_attname)));

            // Check NOT NULL constraint preservation
            if (parent_att->attnotnull && !child_att->attnotnull)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                               errmsg("column \"%s\" in child table must be marked NOT NULL",
                                      parent_attname)));

            // Validate generated column consistency
            if (parent_att->attgenerated && !child_att->attgenerated)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                               errmsg("column \"%s\" in child table must be a generated column", parent_attname)));
            if (child_att->attgenerated && !parent_att->attgenerated)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                               errmsg("column \"%s\" in child table must not be a generated column", parent_attname)));

            // Handle partition-specific inheritance
            if (ispartition)
                child_att->attidentity = parent_att->attidentity;

            // Increment inheritance count
            child_att->attinhcount++;
            if (child_att->attinhcount < 0)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                               errmsg("too many inheritance parents")));

            // Set partition inheritance properties
            if (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
            {
                Assert(child_att->attinhcount == 1);
                child_att->attislocal = false;
            }

            CatalogTupleUpdate(attrrel, &tuple->t_self, tuple);
            heap_freetuple(tuple);
        }
        else
        {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("child table is missing column \"%s\"", parent_attname)));
        }
    }

    table_close(attrrel, RowExclusiveLock);
}
```