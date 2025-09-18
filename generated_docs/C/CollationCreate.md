# CollationCreate

## Location
src/backend/catalog/pg_collation.c: 42 - 236

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
  - namestrcpy
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - pg_encoding_to_char
- Called from (representative examples):
  - [DefineCollation](../D/DefineCollation.md)
  - [pg_import_system_collations](../p/pg_import_system_collations.md)

## Notes and Other Information
The function implements sophisticated duplicate detection by checking both specific-encoding and any-encoding collations to prevent shadowing. It uses ShareRowExclusiveLock to protect against race conditions during duplicate checks. The function supports both libc and ICU collation providers with different parameter requirements. Extension membership is verified when if_not_exists is used to ensure security. The function is located in src/backend/catalog/pg_collation.c:42-236.