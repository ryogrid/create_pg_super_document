# get_catalog_object_by_oid_extended

## Location
src/backend/catalog/objectaddress.c: 2794 - 2854

## Overview
Extended version of catalog object retrieval that provides fine-grained control over tuple locking behavior for inplace-updated tables.

## Definition


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
  - get_object_catcache_oid (to determine cache availability)
  - SearchSysCacheLockedCopy1 (locked syscache lookup)
  - SearchSysCacheCopy1 (regular syscache lookup)
  - get_object_oid_index (for direct table scan)
  - systable_beginscan, systable_getnext, systable_endscan (table scanning)
  - LockTuple (tuple locking for inplace updates)
  - InplaceUpdateTupleLock (lock mode constant)
  - heap_copytuple (tuple copying)
  - RelationGetRelid, ObjectIdGetDatum, HeapTupleIsValid, ScanKeyInit
- Called from (representative examples):
  - get_catalog_object_by_oid (main wrapper)
  - AlterObjectOwner_internal
  - ObjectAddressSet

## Notes and Other Information
- Provides the core catalog object retrieval logic with locking control
- Supports both syscache and direct index scan methods for maximum flexibility
- The locktup parameter is crucial for safe access to inplace-updated catalog tables
- Returns NULL if object is not found rather than throwing errors
- Located in src/backend/catalog/objectaddress.c:2794-2854
- Always returns a copy of the tuple, requiring caller to manage memory deallocation
- Uses Assert to ensure valid OID index when syscache is unavailable