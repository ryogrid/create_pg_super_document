# LargeObjectDrop

## Location
src/backend/catalog/pg_largeobject.c: 83 - 154

## Overview
Completely removes a large object by deleting both its metadata from pg_largeobject_metadata and all associated data pages from pg_largeobject.

## Definition
```c
void LargeObjectDrop(Oid loid)
```

## Detailed Description
The LargeObjectDrop function performs a complete removal of a large object by deleting entries from both catalog tables that store large object information. It operates in two phases:

1. **Metadata Removal**: Searches for and deletes the large object's metadata entry from pg_largeobject_metadata using the provided OID. If the large object does not exist, it raises an ERROR.

2. **Data Page Removal**: Scans and deletes all data pages associated with the large object from the pg_largeobject table.

The function uses system table scans with appropriate indexes (LargeObjectMetadataOidIndexId and LargeObjectLOidPNIndexId) for efficient lookups. Both relations are opened with RowExclusiveLock to ensure exclusive access during the deletion process.

## Parameters / Member Variables
- `loid`: The OID of the large object to be dropped. Must be a valid large object identifier that exists in the system.

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md) (system scan descriptor type)
  - [systable_beginscan](../s/systable_beginscan.md) (begins system table scan)
  - [systable_getnext](../s/systable_getnext.md) (gets next tuple from system scan)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (deletes tuple from catalog table)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (from src/backend/catalog/dependency.c:1404)

## Notes and Other Information
- Raises ERROR with code ERRCODE_UNDEFINED_OBJECT if the large object does not exist
- Performs complete cleanup by removing both metadata and all data pages
- Uses indexed scans for efficient deletion operations
- Ensures data consistency by using RowExclusiveLock on both relations
- Part of the dependency deletion system - automatically called when objects dependent on large objects are dropped
- Located in src/backend/catalog/pg_largeobject.c:83-154