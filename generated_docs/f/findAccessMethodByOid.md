# findAccessMethodByOid

## Location
[src/bin/pg_dump/common.c:943-960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L943-L960)

## Overview
Finds and returns the DumpableObject for a PostgreSQL access method with the given OID, used in pg_dump operations for access method lookup during database dumping.

## Definition
```c
AccessMethodInfo *findAccessMethodByOid(Oid oid)
```

## Detailed Description
This function is part of the pg_dump utility's object management system. It searches for an access method object by its OID (Object Identifier) and returns a pointer to the corresponding AccessMethodInfo structure. The function creates a CatalogId using the AccessMethodRelationId and the provided OID, then delegates to findObjectByCatalogId to locate the actual DumpableObject. It includes an assertion to ensure that any found object is indeed an access method object (DO_ACCESS_METHOD).

## Parameters / Member Variables
- `oid`: The Object Identifier (OID) of the access method to find in the PostgreSQL system catalogs

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md)
  - [CatalogId](../C/CatalogId.md) (struct)
  - DumpableObject (struct)
  - [AccessMethodInfo](../A/AccessMethodInfo.md) (struct)
  - DO_ACCESS_METHOD (enum value)
  - AccessMethodRelationId (constant)
- Called from (representative examples):
  - [accessMethodNameCompare](../a/accessMethodNameCompare.md) (src/bin/pg_dump/pg_dump_sort.c:523, 524)

## Notes and Other Information
- Returns NULL if no access method with the specified OID is found
- Uses assertions to validate that found objects are actually access method objects
- Part of the pg_dump object lookup infrastructure for database schema dumping
- Works specifically with access method objects (like btree, hash, gin, gist, etc.)
- Located in src/bin/pg_dump/common.c:943-960
- Primarily used in sorting operations and name comparison functions for access methods