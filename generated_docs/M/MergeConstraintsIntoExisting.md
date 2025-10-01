# MergeConstraintsIntoExisting

## Location
[src/backend/commands/tablecmds.c:16016-16140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16016-L16140)

## Overview
MergeConstraintsIntoExisting is a static function that validates constraint compatibility between child and parent relations during inheritance creation, and increments the inheritance count for matching constraints.

## Definition

```c
static void
MergeConstraintsIntoExisting(Relation child_rel, Relation parent_rel)
```
## Detailed Description
This function performs comprehensive constraint validation and merging when establishing inheritance relationships between tables. It ensures that all inheritable check constraints from the parent relation exist in the child relation with equivalent definitions, then updates the inheritance counts accordingly. The function uses a nested scanning approach to compare constraints by name and functional equivalence.

For each check constraint in the parent relation, the function:
1. Skips non-check constraints and NO INHERIT constraints
2. Searches for a matching constraint by name in the child relation
3. Validates that the constraints are functionally equivalent using constraints_equivalent()
4. Ensures the child constraint is not marked as NO INHERIT
5. Validates that validation status is compatible (valid parent cannot merge with invalid child)
6. Increments the constraint's inheritance count (coninhcount)
7. For partitions, sets conislocal to false since partitions cannot have local constraints
8. Updates the catalog with the modified constraint information

The function uses an O(N^2) algorithm but is considered acceptable for typical constraint counts (10-100).

## Parameters / Member Variables
- : The child relation being established as an inheritor
- : The parent relation to inherit constraints from

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - RelationGetRelid
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - GETSTRUCT
  - NameStr
  - strcmp
  - [constraints_equivalent](../c/constraints_equivalent.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
  - ereport
  - RelationGetRelationName
  - RelationGetDescr
  - Form_pg_constraint
  - [SysScanDesc](../S/SysScanDesc.md)
  - CONSTRAINT_CHECK
- Called from (representative examples):
  - [CreateInheritance](../C/CreateInheritance.md)
  - child_dependency_type

## Notes and Other Information
- Currently requires all parent check constraints to exist in child - missing constraints result in an error
- Only processes check constraints (CONSTRAINT_CHECK), ignoring other constraint types
- Ignores parent constraints marked with NO INHERIT flag
- Prevents merging if child constraint is marked NO INHERIT or has incompatible validation status
- Uses RowExclusiveLock on pg_constraint catalog for safe concurrent access
- Algorithm complexity is O(N^2) but acceptable for typical constraint counts
- For partitioned tables, ensures inherited constraints are marked as non-local (conislocal = false)
- Prevents inheritance count overflow by checking for negative values after increment
- All constraint modifications are transactional and will rollback if the operation fails later
- Future consideration mentioned for auto-creating missing constraints like CREATE TABLE

## Simplified Source

```c
static void
MergeConstraintsIntoExisting(Relation child_rel, Relation parent_rel)
{
    Relation constraintrel;
    SysScanDesc parent_scan;
    ScanKeyData parent_key;
    HeapTuple parent_tuple;
    Oid parent_relid = RelationGetRelid(parent_rel);

    constraintrel = table_open(ConstraintRelationId, RowExclusiveLock);

    // Scan parent's constraints
    ScanKeyInit(&parent_key, Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parent_relid));
    parent_scan = systable_beginscan(constraintrel, ConstraintRelidTypidNameIndexId,
                                    true, NULL, 1, &parent_key);

    while (HeapTupleIsValid(parent_tuple = systable_getnext(parent_scan)))
    {
        Form_pg_constraint parent_con = (Form_pg_constraint) GETSTRUCT(parent_tuple);
        SysScanDesc child_scan;
        ScanKeyData child_key;
        HeapTuple child_tuple;
        bool found = false;

        // Only process inheritable check constraints
        if (parent_con->contype != CONSTRAINT_CHECK || parent_con->connoinherit)
            continue;

        // Search for matching constraint in child
        ScanKeyInit(&child_key, Anum_pg_constraint_conrelid,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(RelationGetRelid(child_rel)));
        child_scan = systable_beginscan(constraintrel, ConstraintRelidTypidNameIndexId,
                                       true, NULL, 1, &child_key);

        while (HeapTupleIsValid(child_tuple = systable_getnext(child_scan)))
        {
            Form_pg_constraint child_con = (Form_pg_constraint) GETSTRUCT(child_tuple);
            HeapTuple child_copy;

            // Look for matching check constraint by name
            if (child_con->contype != CONSTRAINT_CHECK ||
                strcmp(NameStr(parent_con->conname), NameStr(child_con->conname)) != 0)
                continue;

            // Validate constraints are equivalent
            if (!constraints_equivalent(parent_tuple, child_tuple, RelationGetDescr(constraintrel)))
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                               errmsg("child table \"%s\" has different definition for check constraint \"%s\"",
                                      RelationGetRelationName(child_rel), NameStr(parent_con->conname))));

            // Validate inheritance compatibility
            if (child_con->connoinherit)
                ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                               errmsg("constraint \"%s\" conflicts with non-inherited constraint on child table \"%s\"",
                                      NameStr(child_con->conname), RelationGetRelationName(child_rel))));

            if (parent_con->convalidated && !child_con->convalidated)
                ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                               errmsg("constraint \"%s\" conflicts with NOT VALID constraint on child table \"%s\"",
                                      NameStr(child_con->conname), RelationGetRelationName(child_rel))));

            // Update inheritance count
            child_copy = heap_copytuple(child_tuple);
            child_con = (Form_pg_constraint) GETSTRUCT(child_copy);
            child_con->coninhcount++;
            if (child_con->coninhcount < 0)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                               errmsg("too many inheritance parents")));

            // For partitions, mark as non-local
            if (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
            {
                Assert(child_con->coninhcount == 1);
                child_con->conislocal = false;
            }

            CatalogTupleUpdate(constraintrel, &child_copy->t_self, child_copy);
            heap_freetuple(child_copy);
            found = true;
            break;
        }

        systable_endscan(child_scan);

        if (!found)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("child table is missing constraint \"%s\"",
                                  NameStr(parent_con->conname))));
    }

    systable_endscan(parent_scan);
    table_close(constraintrel, RowExclusiveLock);
}
```