# CollationCreate

## Location
[src/backend/catalog/pg_collation.c:42-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_collation.c#L42-L236)

## Overview
Creates a new collation object and adds it to the pg_collation system catalog, handling duplicate checks and dependency management.

## Definition
```c
Oid CollationCreate(const char *collname, Oid collnamespace,
                    Oid collowner,
                    char collprovider,
                    bool collisdeterministic,
                    int32 collencoding,
                    const char *collcollate, const char *collctype,
                    const char *colllocale,
                    const char *collicurules,
                    const char *collversion,
                    bool if_not_exists,
                    bool quiet)
```

## Detailed Description
CollationCreate is the core function responsible for creating new collation objects in PostgreSQL. It performs comprehensive validation to ensure no naming conflicts exist, handles both libc and ICU collation providers, and manages all necessary database catalog entries and dependencies. The function implements sophisticated duplicate detection that considers both specific-encoding and any-encoding collations, preventing shadowing conflicts. Upon successful creation, it establishes dependencies on the namespace and owner, integrates with the extension system, and triggers post-creation hooks.

## Parameters / Member Variables
- `collname`: The name of the collation to create
- `collnamespace`: OID of the namespace where the collation will be created
- `collowner`: OID of the user who will own the collation
- `collprovider`: Character indicating the collation provider (e.g., COLLPROVIDER_LIBC)
- `collisdeterministic`: Boolean flag indicating if the collation is deterministic
- `collencoding`: Encoding ID for the collation (-1 for any encoding)
- `collcollate`: LC_COLLATE setting for libc provider (NULL for non-libc)
- `collctype`: LC_CTYPE setting for libc provider (NULL for non-libc)
- `colllocale`: Locale string for non-libc providers (NULL for libc)
- `collicurules`: ICU collation rules (NULL if not applicable)
- `collversion`: Version string for the collation (NULL if not specified)
- `if_not_exists`: If true, print notice and return InvalidOid on duplicate instead of error
- `quiet`: If true, silently return InvalidOid on duplicate (overrides if_not_exists)

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid3
  - ObjectAddressSet
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
  - [heap_freetuple](../h/heap_freetuple.md)
  - [namestrcpy](../n/namestrcpy.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [pg_encoding_to_char](../p/pg_encoding_to_char.md)
- Called from (representative examples):
  - [DefineCollation](../D/DefineCollation.md)
  - [pg_import_system_collations](../p/pg_import_system_collations.md)

## Notes and Other Information
The function implements sophisticated duplicate detection by checking both specific-encoding and any-encoding collations to prevent shadowing. It uses ShareRowExclusiveLock to protect against race conditions during duplicate checks. The function supports both libc and ICU collation providers with different parameter requirements. Extension membership is verified when if_not_exists is used to ensure security. The function is located in src/backend/catalog/pg_collation.c:42-236.

## Simplified Source

```c
// Simplified version of CollationCreate
Oid CollationCreate(const char *collname, Oid collnamespace, Oid collowner,
                    char collprovider, bool collisdeterministic, int32 collencoding,
                    const char *collcollate, const char *collctype, const char *colllocale,
                    const char *collicurules, const char *collversion,
                    bool if_not_exists, bool quiet) {
    Relation rel;
    HeapTuple tup;
    Datum values[Natts_pg_collation];
    bool nulls[Natts_pg_collation];
    NameData name_name;
    Oid oid;
    ObjectAddress myself, referenced;

    // Check for existing collation with same name and encoding
    oid = GetSysCacheOid3(COLLNAMEENCNSP, Anum_pg_collation_oid,
                          PointerGetDatum(collname), Int32GetDatum(collencoding),
                          ObjectIdGetDatum(collnamespace));

    if (OidIsValid(oid)) {
        if (quiet) return InvalidOid;
        if (if_not_exists) {
            // Verify extension membership for security
            ObjectAddressSet(myself, CollationRelationId, oid);
            checkMembershipInCurrentExtension(&myself);
            ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("collation \"%s\" already exists, skipping", collname)));
            return InvalidOid;
        }
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("collation \"%s\" already exists", collname)));
    }

    // Open pg_collation catalog and check for shadowing conflicts
    rel = table_open(CollationRelationId, ShareRowExclusiveLock);

    // Additional check to prevent encoding-specific vs any-encoding conflicts
    if (collencoding == -1) {
        oid = GetSysCacheOid3(COLLNAMEENCNSP, Anum_pg_collation_oid,
                              PointerGetDatum(collname), Int32GetDatum(GetDatabaseEncoding()),
                              ObjectIdGetDatum(collnamespace));
    } else {
        oid = GetSysCacheOid3(COLLNAMEENCNSP, Anum_pg_collation_oid,
                              PointerGetDatum(collname), Int32GetDatum(-1),
                              ObjectIdGetDatum(collnamespace));
    }

    if (OidIsValid(oid)) {
        // Handle shadowing conflict similar to above
        table_close(rel, NoLock);
        if (quiet) return InvalidOid;
        if (if_not_exists) {
            ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("collation \"%s\" already exists, skipping", collname)));
            return InvalidOid;
        }
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("collation \"%s\" already exists", collname)));
    }

    // Create new catalog tuple
    memset(nulls, 0, sizeof(nulls));
    namestrcpy(&name_name, collname);
    oid = GetNewOidWithIndex(rel, CollationOidIndexId, Anum_pg_collation_oid);

    // Fill in tuple values
    values[Anum_pg_collation_oid - 1] = ObjectIdGetDatum(oid);
    values[Anum_pg_collation_collname - 1] = NameGetDatum(&name_name);
    values[Anum_pg_collation_collnamespace - 1] = ObjectIdGetDatum(collnamespace);
    values[Anum_pg_collation_collowner - 1] = ObjectIdGetDatum(collowner);
    values[Anum_pg_collation_collprovider - 1] = CharGetDatum(collprovider);
    values[Anum_pg_collation_collisdeterministic - 1] = BoolGetDatum(collisdeterministic);
    values[Anum_pg_collation_collencoding - 1] = Int32GetDatum(collencoding);

    // Set optional string fields (collcollate, collctype, colllocale, collicurules, collversion)
    if (collcollate) values[Anum_pg_collation_collcollate - 1] = CStringGetTextDatum(collcollate);
    else nulls[Anum_pg_collation_collcollate - 1] = true;
    // ... similar for other optional fields

    // Insert tuple and establish dependencies
    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    CatalogTupleInsert(rel, tup);

    // Set up object dependencies
    myself.classId = CollationRelationId;
    myself.objectId = oid;
    myself.objectSubId = 0;

    // Dependencies on namespace, owner, and current extension
    referenced.classId = NamespaceRelationId;
    referenced.objectId = collnamespace;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    recordDependencyOnOwner(CollationRelationId, oid, collowner);
    recordDependencyOnCurrentExtension(&myself, false);

    // Cleanup and post-creation hook
    InvokeObjectPostCreateHook(CollationRelationId, oid, 0);
    heap_freetuple(tup);
    table_close(rel, NoLock);

    return oid;
}
```

Key simplifications made:
- Consolidated duplicate error handling logic
- Removed verbose duplicate checking comments
- Simplified the tuple value assignment (showing pattern but not all fields)
- Streamlined the dependency creation process
- Focused on the main flow: check duplicates → create tuple → establish dependencies
- Preserved essential validation and security checks
- Maintained the core catalog manipulation logic