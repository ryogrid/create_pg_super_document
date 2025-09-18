# findNamespaceByOid

## Location
[src/bin/pg_dump/common.c:979-996](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L979-L996)

## Overview
Finds and returns the DumpableObject for a PostgreSQL namespace (schema) with the specified OID during the pg_dump process.

## Definition


## Detailed Description
This function is part of the pg_dump utility's object lookup system. It searches for a namespace (schema) object by its Object Identifier (OID) and returns the corresponding NamespaceInfo structure. The function works by creating a CatalogId structure with the namespace's OID and using the generic findObjectByCatalogId function to locate the object. It includes an assertion to verify that any found object is indeed of type DO_NAMESPACE, ensuring type safety in the dump process.

## Parameters / Member Variables
- `oid`: The Object Identifier (OID) of the namespace to find

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md)
  - [CatalogId](../C/CatalogId.md) (struct)
  - DumpableObject (struct)
  - [NamespaceInfo](../N/NamespaceInfo.md) (struct)
  - DO_NAMESPACE (enum value)
  - NamespaceRelationId (constant)
- Called from (representative examples):
  - [getPublicationNamespaces](../g/getPublicationNamespaces.md) (src/bin/pg_dump/pg_dump.c:4485)
  - [findNamespace](findNamespace.md) (src/bin/pg_dump/pg_dump.c:5758)

## Notes and Other Information
- Returns NULL if the namespace with the given OID is not found
- Uses an assertion to ensure type safety - the found object must be of DO_NAMESPACE type
- Part of the pg_dump utility's internal object management system
- The function is specific to namespace objects and cannot be used for other database object types