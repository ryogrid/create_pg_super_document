# AlterDomainDropConstraint

## Location
[src/backend/commands/typecmds.c:2791-2896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2791-L2896)

## Overview
Implements the ALTER DOMAIN DROP CONSTRAINT statement, removing a named constraint from a domain type and updating the domain's metadata accordingly.

## Definition

```c
struct = (Form_pg_constraint) GETSTRUCT(contup);
```
## Detailed Description
This function removes a named constraint from a domain type by scanning the pg_constraint catalog for the target constraint. It handles special processing for NOT NULL constraints by updating the typnotnull field in pg_type. The function uses a systematic scan of constraints associated with the domain and performs deletion using the specified drop behavior. It also handles cache invalidation to ensure dependent plans are rebuilt since the domain's pg_type row doesn't change automatically.

## Parameters / Member Variables
- : List of qualified names identifying the domain
- : Name of the constraint to drop
- : Drop behavior (CASCADE or RESTRICT) controlling how dependent objects are handled
- : If true, don't error when the constraint doesn't exist, just issue a notice

## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - SearchSysCacheCopy1
  - [checkDomainOwner](../c/checkDomainOwner.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [performDeletion](../p/performDeletion.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
  - [TypeNameToString](../T/TypeNameToString.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Uses a three-key scan on pg_constraint to find the target constraint efficiently
- Special handling for NOT NULL constraints updates the domain's typnotnull field
- Manually invalidates cache since pg_type row doesn't change for most constraint types
- Supports IF EXISTS semantics through the missing_ok parameter
- Ensures proper locking on both type and constraint relations for consistency

## Simplified Source

```c
ObjectAddress
AlterDomainDropConstraint(List *names, const char *constrName,
                         DropBehavior behavior, bool missing_ok)
{
    TypeName *typename;
    Oid domainoid;
    HeapTuple tup;
    Relation rel;
    Relation conrel;
    SysScanDesc conscan;
    ScanKeyData skey[3];
    HeapTuple contup;
    bool found = false;
    ObjectAddress address;

    // Convert name list to typename and resolve domain OID
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typename);

    // Open type catalog and get domain tuple
    rel = table_open(TypeRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
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
    conscan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, NULL, 3, skey);

    if ((contup = systable_getnext(conscan)) != NULL) {
        Form_pg_constraint construct = (Form_pg_constraint) GETSTRUCT(contup);
        ObjectAddress conobj;

        // Special handling for NOT NULL constraints
        if (construct->contype == CONSTRAINT_NOTNULL) {
            ((Form_pg_type) GETSTRUCT(tup))->typnotnull = false;
            CatalogTupleUpdate(rel, &tup->t_self, tup);
        }

        // Delete the constraint
        conobj.classId = ConstraintRelationId;
        conobj.objectId = construct->oid;
        conobj.objectSubId = 0;

        performDeletion(&conobj, behavior, 0);
        found = true;
    }

    // Clean up scan
    systable_endscan(conscan);
    table_close(conrel, RowExclusiveLock);

    // Handle not found case
    if (!found) {
        if (!missing_ok)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("constraint \"%s\" of domain \"%s\" does not exist",
                           constrName, TypeNameToString(typename))));
        else
            ereport(NOTICE, (errmsg("constraint \"%s\" of domain \"%s\" does not exist, skipping",
                                   constrName, TypeNameToString(typename))));
    }

    // Invalidate cache for dependent plans
    CacheInvalidateHeapTuple(rel, tup, NULL);

    ObjectAddressSet(address, TypeRelationId, domainoid);

    // Clean up
    table_close(rel, RowExclusiveLock);

    return address;
}
```