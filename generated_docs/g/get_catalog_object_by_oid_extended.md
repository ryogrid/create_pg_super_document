# get_catalog_object_by_oid_extended

## Location
[src/backend/catalog/objectaddress.c:2794-2854](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2794-L2854)

## Overview
Extended version of catalog object retrieval that provides fine-grained control over tuple locking behavior for inplace-updated tables.

## Definition

```c
HeapTuple
get_catalog_object_by_oid_extended(Relation catalog,
								   AttrNumber oidcol,
								   Oid objectId,
								   bool locktup)
```
## Detailed Description
This function is the core implementation for retrieving catalog objects by OID, offering more control than the basic get_catalog_object_by_oid wrapper. It supports two retrieval methods: system cache (syscache) lookup when available, or direct table scanning when no appropriate cache exists.

The key feature is the locktup parameter, which controls whether to acquire a LOCKTAG_TUPLE lock at InplaceUpdateTupleLock mode. This is essential for tables that use inplace updates, ensuring safe concurrent access during tuple modifications. When using syscache, it chooses between SearchSysCacheLockedCopy1 and SearchSysCacheCopy1 based on the locking requirement.

For relations without syscache support, the function performs an index scan using the object's OID index, then optionally locks the tuple before making a copy.

## Parameters / Member Variables
- `catalog`: Open Relation representing the catalog table to search (must be opened and locked by caller)
- `oidcol`: Column number (AttrNumber) containing the object OID within the catalog table
- `objectId`: The OID of the object to retrieve from the catalog
- `locktup`: Boolean flag controlling whether to acquire LOCKTAG_TUPLE lock for inplace update safety

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_catcache_oid](get_object_catcache_oid.md) (to determine cache availability)
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md) (locked syscache lookup)
  - SearchSysCacheCopy1 (regular syscache lookup)
  - [get_object_oid_index](get_object_oid_index.md) (for direct table scan)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext, systable_endscan (table scanning)
  - [LockTuple](../L/LockTuple.md) (tuple locking for inplace updates)
  - InplaceUpdateTupleLock (lock mode constant)
  - [heap_copytuple](../h/heap_copytuple.md) (tuple copying)
  - RelationGetRelid, ObjectIdGetDatum, HeapTupleIsValid, ScanKeyInit
- Called from (representative examples):
  - [get_catalog_object_by_oid](get_catalog_object_by_oid.md) (main wrapper)
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md)
  - ObjectAddressSet

## Notes and Other Information
- Provides the core catalog object retrieval logic with locking control
- Supports both syscache and direct index scan methods for maximum flexibility
- The locktup parameter is crucial for safe access to inplace-updated catalog tables
- Returns NULL if object is not found rather than throwing errors
- Located in src/backend/catalog/objectaddress.c:2794-2854
- Always returns a copy of the tuple, requiring caller to manage memory deallocation
- Uses Assert to ensure valid OID index when syscache is unavailable