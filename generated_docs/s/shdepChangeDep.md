# shdepChangeDep

## Location
[src/backend/catalog/pg_shdepend.c:206-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L206-L315)

## Overview
Internal function that updates shared dependency records when the referenced object changes, handling ownership and tablespace dependency modifications.

## Definition


## Detailed Description
This is a core internal function that handles updating pg_shdepend entries when a referenced shared object changes (such as during owner or tablespace changes). It performs intelligent dependency management by: 1) searching for existing dependency entries, 2) handling pinned objects appropriately (not creating dependencies for them), 3) updating existing entries or creating new ones as needed, and 4) cleaning up when dependencies are no longer required. The function ensures there is only one entry per dependent object and dependency type.

## Parameters / Member Variables
- : Already opened pg_shdepend relation with appropriate lock
- : OID of the catalog containing the dependent object
- : OID of the dependent object
- : Sub-object ID (typically 0 for most objects)
- : OID of the catalog containing the new referenced object
- : OID of the new referenced object
- : Type of shared dependency (SHARED_DEPENDENCY_OWNER or SHARED_DEPENDENCY_TABLESPACE)

## Dependencies
- Functions called/Symbols referenced:
  - [classIdGetDbId](../c/classIdGetDbId.md)
  - [shdepLockAndCheckObject](shdepLockAndCheckObject.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](systable_beginscan.md)
  - [systable_getnext](systable_getnext.md)
  - [systable_endscan](systable_endscan.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [IsPinnedObject](../I/IsPinnedObject.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md)
  - [changeDependencyOnTablespace](../c/changeDependencyOnTablespace.md)

## Notes and Other Information
- Static function - internal use only within pg_shdepend.c
- Enforces single dependency entry constraint - errors if multiple matches found
- Handles three scenarios: update existing entry, delete entry (for pinned objects), or insert new entry
- Uses heap_copytuple to make modifiable copies of catalog tuples
- Properly locks referenced objects to prevent them from being dropped during the operation
- Located in src/backend/catalog/pg_shdepend.c:206-315