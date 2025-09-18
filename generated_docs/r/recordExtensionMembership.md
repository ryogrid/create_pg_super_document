# recordExtensionMembership

## Location
src/bin/pg_dump/common.c: 1052 - 1075

## Overview
Records the membership relationship between a database object and a PostgreSQL extension in the pg_dump utility's internal tracking system.

## Definition
```c
void recordExtensionMembership(CatalogId catId, ExtensionInfo *ext)
```

## Detailed Description
This function is a crucial part of the pg_dump utility's extension membership tracking system. It establishes a relationship between a database object (identified by its CatalogId) and the extension it belongs to. This information is essential for properly handling extension dependencies during database dump and restore operations. The function works by inserting or updating an entry in the catalogIdHash table, which maintains the mapping between catalog IDs and their associated extension information. When an object is found to be a member of an extension, this function ensures that the relationship is properly recorded so that the dump process can handle extension dependencies correctly.

## Parameters / Member Variables
- `catId`: The CatalogId structure identifying the database object (contains tableoid and oid)
- `ext`: Pointer to the ExtensionInfo structure representing the extension that owns the object

## Dependencies
- Functions called/Symbols referenced:
  - catalogid_insert (hash table insertion function)
  - catalogIdHash (global hash table variable)
  - [CatalogId](../C/CatalogId.md) (struct)
  - [ExtensionInfo](../E/ExtensionInfo.md) (struct)
  - [CatalogIdMapEntry](../C/CatalogIdMapEntry.md) (struct)
- Called from (representative examples):
  - [getExtensionMembership](../g/getExtensionMembership.md) (src/bin/pg_dump/pg_dump.c:18332)

## Notes and Other Information
- The function assumes that the catalogIdHash table has been properly initialized
- Uses assertions to verify that the hash table exists and that objects don't already have extension membership recorded
- If the catalog ID entry doesn't exist, it creates a new one with NULL dobj and ext fields initially
- Critical for maintaining extension dependency information during pg_dump operations
- Helps ensure that extension-owned objects are properly handled during database restoration
- The function is void and modifies the global catalogIdHash state