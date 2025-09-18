# findExtensionByOid

## Location
src/bin/pg_dump/common.c: 997 - 1014

## Overview
Finds and returns the DumpableObject for a PostgreSQL extension with the specified OID during the pg_dump process.

## Definition
```c
ExtensionInfo *findExtensionByOid(Oid oid)
```

## Detailed Description
This function is part of the pg_dump utility's object lookup system for PostgreSQL extensions. It searches for an extension object by its Object Identifier (OID) and returns the corresponding ExtensionInfo structure. The function operates by creating a CatalogId structure with the extension's OID and utilizing the generic findObjectByCatalogId function to locate the object. It includes an assertion to verify that any found object is indeed of type DO_EXTENSION, ensuring type safety during the dump process.

## Parameters / Member Variables
- `oid`: The Object Identifier (OID) of the extension to find

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md)
  - [CatalogId](../C/CatalogId.md) (struct)
  - DumpableObject (struct)
  - [ExtensionInfo](../E/ExtensionInfo.md) (struct)
  - DO_EXTENSION (enum value)
  - ExtensionRelationId (constant)
- Called from (representative examples):
  - [getExtensionMembership](../g/getExtensionMembership.md) (src/bin/pg_dump/pg_dump.c:18323)

## Notes and Other Information
- Returns NULL if the extension with the given OID is not found
- Uses an assertion to ensure type safety - the found object must be of DO_EXTENSION type
- Part of the pg_dump utility's internal object management system for handling PostgreSQL extensions
- Extensions are add-on modules that extend PostgreSQL's functionality
- The function is specific to extension objects and follows the same pattern as other findXXXByOid functions