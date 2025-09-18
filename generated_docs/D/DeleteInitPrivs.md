# DeleteInitPrivs

## Location
src/backend/catalog/dependency.c: 2785 - 2823

## Overview
A static function that removes initial privileges (ACL entries) from the pg_init_privs catalog for a specific database object, typically called during object deletion.

## Definition
```c
static void DeleteInitPrivs(const ObjectAddress *object)
```

## Detailed Description
This function removes initial privilege entries from PostgreSQL's pg_init_privs system catalog for a specified database object. The pg_init_privs catalog stores the initial privileges that were recorded when an object was created as part of an extension. This cleanup function is essential for maintaining catalog consistency when objects are deleted, ensuring that orphaned privilege records do not remain in the system catalog.

The function performs a systematic scan of the pg_init_privs table using the object's identifiers and deletes all matching privilege records. It handles both regular objects and sub-objects (like table columns) through conditional key scanning.

## Parameters / Member Variables
- `object`: Pointer to an ObjectAddress structure containing:
  - `classId`: The catalog class ID (from pg_class) identifying the type of object
  - `objectId`: The OID of the specific object
  - `objectSubId`: Sub-object identifier (e.g., column number) or 0 for main objects

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens pg_init_privs relation)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes scan keys for catalog search)
  - [systable_beginscan](../s/systable_beginscan.md) (begins system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (gets next tuple from scan)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (deletes catalog tuple)
  - [systable_endscan](../s/systable_endscan.md) (ends catalog scan)
  - table_close (closes relation)
  - [SysScanDesc](../S/SysScanDesc.md) (scan descriptor type)
- Called from:
  - [deleteOneObject](../d/deleteOneObject.md) (main object deletion function)

## Notes and Other Information
- This is a static function, only accessible within dependency.c
- Uses RowExclusiveLock to ensure exclusive access during deletion
- Handles both main objects (objectSubId = 0) and sub-objects (objectSubId > 0) with conditional key scanning
- Part of PostgreSQL's extension privilege management system
- Critical for maintaining catalog consistency during object deletion operations
- The function scans using InitPrivsObjIndexId for efficient lookup by object identifiers