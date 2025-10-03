# GetParentedForeignKeyRefs

## Location
[src/backend/commands/tablecmds.c:20128-20180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L20128-L20180)

## Overview
Retrieves a list of foreign key constraint OIDs that reference the given partition table and are part of a partitioned constraint hierarchy (having parent constraints).

## Definition

```c
static List *
GetParentedForeignKeyRefs(Relation partition)
```
## Detailed Description
This function searches for foreign key constraints that reference the specified partition table and are part of a larger partitioned constraint system. It performs the following operations:

1. **Early Optimization**: Checks if the partition has any indexes or referenceable columns - if not, returns immediately since foreign keys require indexes on the referenced table
2. **Constraint Scanning**: Searches the pg_constraint catalog for foreign key constraints where:
   - The referenced table (confrelid) is the given partition
   - The constraint type is CONSTRAINT_FOREIGN
   - The constraint has a parent constraint (conparentid is valid)
3. **Filtering**: Only includes constraints that are part of a partitioned constraint hierarchy (have a parent constraint)
4. **Result Building**: Constructs and returns a list of constraint OIDs

The function is optimized to avoid unnecessary catalog scans when the partition cannot possibly be referenced by foreign keys (no indexes or no key columns).

## Parameters / Member Variables
- `partition`: The partition relation to find foreign key references for
## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)
  - bms_is_empty
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [lappend_oid](../l/lappend_oid.md)
  - RelationGetRelid
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [CharGetDatum](../C/CharGetDatum.md)
- Called from (representative examples):
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - [ATDetachCheckNoForeignKeyRefs](../A/ATDetachCheckNoForeignKeyRefs.md)

## Notes and Other Information
- The function uses a sequential scan of pg_constraint since there's no suitable index for this specific query pattern
- Only returns constraints that are part of partitioned constraint hierarchies (those with parent constraints)
- Performance is optimized by early exit when the partition has no indexes or referenceable columns
- The INDEX_ATTR_BITMAP_KEY is used to check for columns that can be referenced by foreign keys
- Used primarily during partition detachment operations to check for constraint dependencies
- Returns NIL (empty list) if no matching constraints are found
- The function uses AccessShareLock for reading the constraint catalog to ensure consistency

## Simplified Source

```c
static List *GetParentedForeignKeyRefs(Relation partition) {
    List *constraints = NIL;

    // Early exit: if no indexes or referenceable columns, no FK refs possible
    if (RelationGetIndexList(partition) == NIL ||
        bms_is_empty(RelationGetIndexAttrBitmap(partition, INDEX_ATTR_BITMAP_KEY)))
        return NIL;

    // Open constraint catalog and set up scan keys
    Relation pg_constraint = table_open(ConstraintRelationId, AccessShareLock);
    ScanKeyData key[2];
    ScanKeyInit(&key[0], Anum_pg_constraint_confrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(partition)));
    ScanKeyInit(&key[1], Anum_pg_constraint_contype, BTEqualStrategyNumber,
                F_CHAREQ, CharGetDatum(CONSTRAINT_FOREIGN));

    // Scan for foreign keys referencing this partition
    SysScanDesc scan = systable_beginscan(pg_constraint, InvalidOid, true, NULL, 2, key);
    HeapTuple tuple;
    while ((tuple = systable_getnext(scan)) != NULL) {
        Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(tuple);

        // Only include constraints that have parent constraints (partitioned)
        if (OidIsValid(constraint->conparentid))
            constraints = lappend_oid(constraints, constraint->oid);
    }

    // Clean up and return constraint list
    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);
    return constraints;
}
```