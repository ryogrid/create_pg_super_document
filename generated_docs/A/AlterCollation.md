# AlterCollation

## Location
[src/backend/commands/collationcmds.c:428-510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/collationcmds.c#L428-L510)

## Overview
AlterCollation implements the ALTER COLLATION REFRESH VERSION command, updating a collation's version information to reflect changes in the underlying locale library.

## Definition

```c
ObjectAddress
AlterCollation(AlterCollationStmt *stmt)
```
## Detailed Description
This function handles the ALTER COLLATION REFRESH VERSION SQL command by:
1. Validating that the target collation exists and the user has ownership privileges
2. Preventing alteration of the default collation (suggesting ALTER DATABASE instead)
3. Retrieving the current version information from the system catalog
4. Obtaining the actual current version from the collation provider
5. Comparing versions and updating the catalog entry if they differ
6. Providing user feedback through NOTICE messages about version changes

The function ensures version consistency between PostgreSQL's catalog and the underlying collation library, which is important for detecting potential collation behavior changes that could affect index integrity.

## Parameters / Member Variables
- `*stmt`: AlterCollationStmt structure containing the collation name to refresh
## Dependencies
- Functions called/Symbols referenced:
  - [get_collation_oid](../g/get_collation_oid.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [NameListToString](../N/NameListToString.md)
  - [get_collation_actual_version](../g/get_collation_actual_version.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Only supports REFRESH VERSION operation (other ALTER COLLATION operations are handled by generic alter functions)
- Prevents modification of DEFAULT_COLLATION_OID with helpful hint to use ALTER DATABASE instead
- Handles both libc and ICU collation providers by checking collForm->collprovider
- Validates that version changes are logical (cannot change from NULL to non-NULL or vice versa)
- Provides user-friendly NOTICE messages indicating whether the version changed or remained the same
- Uses heap_modify_tuple for atomic catalog updates and triggers appropriate post-alter hooks

## Simplified Source

```c
ObjectAddress
AlterCollation(AlterCollationStmt *stmt)
{
    Relation rel;
    Oid collOid;
    HeapTuple tup;
    Form_pg_collation collForm;
    char *oldversion;
    char *newversion;
    ObjectAddress address;

    // Open collation catalog with exclusive lock
    rel = table_open(CollationRelationId, RowExclusiveLock);
    collOid = get_collation_oid(stmt->collname, false);

    // Prevent altering default collation
    if (collOid == DEFAULT_COLLATION_OID)
        ereport(ERROR, (errmsg("cannot refresh version of default collation"),
                errhint("Use %s instead.", "ALTER DATABASE ... REFRESH COLLATION VERSION")));

    // Check ownership permissions
    if (!object_ownercheck(CollationRelationId, collOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_COLLATION, NameListToString(stmt->collname));

    // Get collation tuple from catalog
    tup = SearchSysCacheCopy1(COLLOID, ObjectIdGetDatum(collOid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for collation %u", collOid);

    collForm = (Form_pg_collation) GETSTRUCT(tup);

    // Extract current version from catalog
    datum = SysCacheGetAttr(COLLOID, tup, Anum_pg_collation_collversion, &isnull);
    oldversion = isnull ? NULL : TextDatumGetCString(datum);

    // Get collation locale string based on provider
    if (collForm->collprovider == COLLPROVIDER_LIBC)
        datum = SysCacheGetAttrNotNull(COLLOID, tup, Anum_pg_collation_collcollate);
    else
        datum = SysCacheGetAttrNotNull(COLLOID, tup, Anum_pg_collation_colllocale);

    // Get actual current version from provider
    newversion = get_collation_actual_version(collForm->collprovider, TextDatumGetCString(datum));

    // Validate version change logic
    if ((!oldversion && newversion) || (oldversion && !newversion))
        elog(ERROR, "invalid collation version change");
    else if (oldversion && newversion && strcmp(newversion, oldversion) != 0) {
        // Version has changed - update catalog
        bool nulls[Natts_pg_collation];
        bool replaces[Natts_pg_collation];
        Datum values[Natts_pg_collation];

        ereport(NOTICE, (errmsg("changing version from %s to %s", oldversion, newversion)));

        // Set up tuple modification
        memset(values, 0, sizeof(values));
        memset(nulls, false, sizeof(nulls));
        memset(replaces, false, sizeof(replaces));

        values[Anum_pg_collation_collversion - 1] = CStringGetTextDatum(newversion);
        replaces[Anum_pg_collation_collversion - 1] = true;

        tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
    } else
        ereport(NOTICE, (errmsg("version has not changed")));

    // Update catalog and trigger hooks
    CatalogTupleUpdate(rel, &tup->t_self, tup);
    InvokeObjectPostAlterHook(CollationRelationId, collOid, 0);

    // Prepare return value
    ObjectAddressSet(address, CollationRelationId, collOid);

    // Clean up
    heap_freetuple(tup);
    table_close(rel, NoLock);

    return address;
}
```