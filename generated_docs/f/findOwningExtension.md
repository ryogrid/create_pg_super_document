# findOwningExtension

## Location
src/bin/pg_dump/common.c: 1076 - 1099

## Overview
Finds the owning extension for a specified catalog object ID in pg_dump, returning NULL if the object is not owned by any extension.

## Definition
ExtensionInfo *findOwningExtension(CatalogId catalogId)

## Detailed Description
This function is part of pg_dump's extension tracking system. It searches the catalog ID hash table to determine if a given database object (identified by its catalog ID) is owned by an extension. The function performs a lookup in the global catalogIdHash table and returns the associated ExtensionInfo structure if found, or NULL if the object is not extension-owned or if no objects have been cataloged yet.

## Parameters / Member Variables
- `catalogId`: A CatalogId structure identifying the database object to check for extension ownership

## Dependencies
- Functions called/Symbols referenced:
  - catalogid_lookup
- Data types used:
  - CatalogId
  - CatalogIdMapEntry
  - ExtensionInfo
- Called from (representative examples):
  - checkExtensionMembership
  - SubRelInfo

## Notes and Other Information
- Returns NULL if catalogIdHash is NULL, indicating no objects have been processed yet
- Part of pg_dump's extension membership tracking system
- Used to determine whether objects should be dumped as part of extension definitions or as standalone objects
- The function is lightweight and performs only a hash table lookup operation