# get_index_constraint

## Location
[src/backend/catalog/pg_depend.c:989-1044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L989-L1044)

## Overview
Retrieves the OID of the constraint (unique, primary key, or exclusion) that owns a given index, returning InvalidOid if no owning constraint exists.

## Definition
Oid get_index_constraint(Oid indexId)

## Detailed Description
This function searches the PostgreSQL dependency system to find the constraint that owns a specific index. It scans the pg_depend system catalog to locate internal dependencies between the index and any constraint objects. The function specifically looks for constraints of types unique, primary key, or exclusion that have an internal dependency relationship with the given index. This is essential for understanding the relationship between indexes and their associated constraints in PostgreSQL's constraint management system.

## Parameters / Member Variables
- `indexId`: The OID of the index for which to find the owning constraint

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_depend
  - DEPENDENCY_INTERNAL
- Called from (representative examples):
  - [index_concurrently_swap](../i/index_concurrently_swap.md)
  - [RenameRelationInternal](../R/RenameRelationInternal.md)
  - [RememberIndexForRebuilding](../R/RememberIndexForRebuilding.md)
  - [generateClonedIndexStmt](generateClonedIndexStmt.md)
  - [transformIndexConstraint](../t/transformIndexConstraint.md)

## Notes and Other Information
The function performs a catalog scan on pg_depend using the DependDependerIndexId index for efficient lookup. It specifically searches for internal dependencies where the index is the dependent object and a constraint is the referenced object. This relationship is crucial for PostgreSQL's constraint system, as constraints like primary keys and unique constraints are implemented using indexes. The function returns InvalidOid when no constraint owns the index, which is the case for indexes created independently of constraints.

## Simplified Source

```c
Oid get_index_constraint(Oid indexId)
{
    Oid constraintId = InvalidOid;
    Relation depRel;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple tup;

    // Open the dependency table
    depRel = table_open(DependRelationId, AccessShareLock);

    // Set up scan keys to find dependencies for this index
    ScanKeyInit(&key[0], Anum_pg_depend_classid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(indexId));
    ScanKeyInit(&key[2], Anum_pg_depend_objsubid,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(0));

    // Scan for dependency entries
    scan = systable_beginscan(depRel, DependDependerIndexId, true,
                             NULL, 3, key);

    // Look for internal dependency on a constraint
    while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

        if (deprec->refclassid == ConstraintRelationId &&
            deprec->refobjsubid == 0 &&
            deprec->deptype == DEPENDENCY_INTERNAL)
        {
            constraintId = deprec->refobjid;
            break;
        }
    }

    systable_endscan(scan);
    table_close(depRel, AccessShareLock);
    return constraintId;
}
```