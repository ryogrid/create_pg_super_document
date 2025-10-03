# AlterDomainValidateConstraint

## Location
[src/backend/commands/typecmds.c:3037-3135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3037-L3135)

## Overview
Implements the ALTER DOMAIN VALIDATE CONSTRAINT statement, validating an existing check constraint on a domain and marking it as validated in the catalog.

## Definition

```c
ObjectAddress
AlterDomainValidateConstraint(List *names, const char *constrName)
```
## Detailed Description
This function validates an existing check constraint on a domain type by first locating the constraint in pg_constraint, verifying it's a check constraint, extracting the constraint expression, and running validation against all existing data. After successful validation, it updates the constraint's convalidated flag to true in the catalog. The function ensures proper constraint validation semantics while maintaining catalog consistency and triggering appropriate hooks for change notification.

## Parameters / Member Variables
- `*names`: List of qualified names identifying the domain containing the constraint
- `*constrName`: Name of the check constraint to validate
## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [checkDomainOwner](../c/checkDomainOwner.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - [validateDomainCheckConstraint](../v/validateDomainCheckConstraint.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [TypeNameToString](../T/TypeNameToString.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Only works with check constraints, rejects other constraint types with appropriate error messages
- Uses a three-key scan to efficiently locate the target constraint in pg_constraint
- Validates all existing domain values against the constraint expression before marking as validated
- Updates the convalidated flag in a copied tuple to ensure proper catalog semantics
- Properly manages system cache and relation locks throughout the operation
- Triggers post-alter hooks for proper event notification in the constraint system

## Simplified Source

```c
ObjectAddress
AlterDomainValidateConstraint(List *names, const char *constrName)
{
    TypeName *typename;
    Oid domainoid;
    Relation typrel;
    Relation conrel;
    HeapTuple tup;
    Form_pg_constraint con;
    Form_pg_constraint copy_con;
    char *conbin;
    SysScanDesc scan;
    HeapTuple tuple;
    HeapTuple copyTuple;
    ScanKeyData skey[3];
    ObjectAddress address;

    // Convert name list to typename and resolve domain OID
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typename);

    // Open type catalog and get domain tuple
    typrel = table_open(TypeRelationId, AccessShareLock);
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(domainoid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", domainoid);

    // Check domain ownership permissions
    checkDomainOwner(tup);

    // Open constraint catalog for scanning
    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    // Set up scan keys to find the constraint
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(domainoid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(constrName));

    // Scan for the target constraint
    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, NULL, 3, skey);

    if (!HeapTupleIsValid(tuple = systable_getnext(scan)))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("constraint \"%s\" of domain \"%s\" does not exist",
                       constrName, TypeNameToString(typename))));

    con = (Form_pg_constraint) GETSTRUCT(tuple);

    // Verify it's a check constraint
    if (con->contype != CONSTRAINT_CHECK)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("constraint \"%s\" of domain \"%s\" is not a check constraint",
                       constrName, TypeNameToString(typename))));

    // Extract constraint expression
    val = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conbin);
    conbin = TextDatumGetCString(val);

    // Validate all existing domain values against the constraint
    validateDomainCheckConstraint(domainoid, conbin);

    // Update catalog to mark constraint as validated
    copyTuple = heap_copytuple(tuple);
    copy_con = (Form_pg_constraint) GETSTRUCT(copyTuple);
    copy_con->convalidated = true;
    CatalogTupleUpdate(conrel, &copyTuple->t_self, copyTuple);

    // Trigger post-alter hooks
    InvokeObjectPostAlterHook(ConstraintRelationId, con->oid, 0);

    ObjectAddressSet(address, TypeRelationId, domainoid);

    // Clean up
    heap_freetuple(copyTuple);
    systable_endscan(scan);
    table_close(typrel, AccessShareLock);
    table_close(conrel, RowExclusiveLock);
    ReleaseSysCache(tup);

    return address;
}
```