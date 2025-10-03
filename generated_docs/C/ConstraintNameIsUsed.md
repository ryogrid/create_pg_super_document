# ConstraintNameIsUsed

## Location
[src/backend/catalog/pg_constraint.c:399-443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L399-L443)

## Overview
Tests whether a given constraint name is currently in use for a specific object (relation or domain) to determine if a user-specified constraint name is acceptable.

## Definition

```c
bool
ConstraintNameIsUsed(ConstraintCategory conCat, Oid objId,
					 const char *conname)
```
## Detailed Description
This function checks if a constraint name is already being used on a specific object (table, index, or domain). Unlike ChooseConstraintName which avoids names used anywhere in the namespace, this function only prevents duplicate constraint names on the same object. It performs a catalog scan of pg_constraint using the appropriate index to efficiently locate any existing constraint with the same name on the specified object. The function is designed to validate user-provided constraint names during DDL operations.

## Parameters / Member Variables
- `conCat`: Category of constraint - either CONSTRAINT_RELATION for table constraints or CONSTRAINT_DOMAIN for domain constraints
- `objId`: OID of the object (relation or domain) to check constraint names against
- `*conname`: Name of the constraint to check for existence
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - HeapTupleIsValid
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [CStringGetDatum](CStringGetDatum.md)
- Called from (representative examples):
  - [index_create](../i/index_create.md)
  - [RenameConstraintById](../R/RenameConstraintById.md)
  - [ATExecAddConstraint](../A/ATExecAddConstraint.md)
  - [addFkConstraint](../a/addFkConstraint.md)
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md)
  - [domainAddNotNullConstraint](../d/domainAddNotNullConstraint.md)

## Notes and Other Information
- Returns true if the constraint name is already used, false otherwise
- Requires exclusive lock on the target object to prevent race conditions with concurrent constraint additions
- Uses ConstraintRelidTypidNameIndexId for efficient scanning
- Only checks for name conflicts on the same object, unlike system-generated name checking
- Part of the constraint name validation process during DDL operations

## Simplified Source

```c
bool ConstraintNameIsUsed(ConstraintCategory conCat, Oid objId, const char *conname)
{
    bool found;
    Relation conDesc;
    SysScanDesc conscan;
    ScanKeyData skey[3];

    // Open pg_constraint catalog
    conDesc = table_open(ConstraintRelationId, AccessShareLock);

    // Set up scan keys for constraint lookup
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum((conCat == CONSTRAINT_RELATION) ? objId : InvalidOid));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum((conCat == CONSTRAINT_DOMAIN) ? objId : InvalidOid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(conname));

    // Scan for matching constraint
    conscan = systable_beginscan(conDesc, ConstraintRelidTypidNameIndexId, true, NULL, 3, skey);

    // Check if constraint exists (at most one should match)
    found = (HeapTupleIsValid(systable_getnext(conscan)));

    // Cleanup
    systable_endscan(conscan);
    table_close(conDesc, AccessShareLock);

    return found;
}
```