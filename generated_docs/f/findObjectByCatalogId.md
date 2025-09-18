# findObjectByCatalogId

## Location
[src/bin/pg_dump/common.c:767-785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L767-L785)

## Overview
Retrieves a DumpableObject by its catalog ID using hash table lookup in the pg_dump catalog ID mapping system.

## Definition


## Detailed Description
This function performs a lookup operation to find a DumpableObject associated with a given CatalogId. It uses the global catalogIdHash hash table to efficiently locate objects based on their PostgreSQL catalog identifiers (typically consisting of table OID and row OID). The function includes safety checks for uninitialized state and returns NULL for unknown catalog IDs. This is a fundamental function in pg_dump's object resolution system, enabling the tool to locate database objects based on their PostgreSQL internal identifiers during the dump process.

## Parameters / Member Variables
- : The CatalogId to look up, representing a PostgreSQL catalog object identifier

## Dependencies
- Functions called/Symbols referenced:
  - catalogid_lookup (hash table lookup function)
  - catalogIdHash (global hash table mapping CatalogIds to objects)
- Data structures used:
  - [CatalogId](../C/CatalogId.md) (parameter type)
  - [CatalogIdMapEntry](../C/CatalogIdMapEntry.md) (intermediate lookup result)
  - DumpableObject (return type)
- Called from (representative examples):
  - [findTableByOid](findTableByOid.md) (src/bin/pg_dump/common.c:859)
  - [findIndexByOid](findIndexByOid.md) (src/bin/pg_dump/common.c:877)
  - [findTypeByOid](findTypeByOid.md) (src/bin/pg_dump/common.c:895)
  - [findFuncByOid](findFuncByOid.md) (src/bin/pg_dump/common.c:914)
  - [findOprByOid](findOprByOid.md) (src/bin/pg_dump/common.c:932)
  - [findAccessMethodByOid](findAccessMethodByOid.md) (src/bin/pg_dump/common.c:950)
  - [findCollationByOid](findCollationByOid.md) (src/bin/pg_dump/common.c:968)
  - [findNamespaceByOid](findNamespaceByOid.md) (src/bin/pg_dump/common.c:986)
  - [buildMatViewRefreshDependencies](../b/buildMatViewRefreshDependencies.md) (src/bin/pg_dump/pg_dump.c:2966, 2978)
  - [getAdditionalACLs](../g/getAdditionalACLs.md) (src/bin/pg_dump/pg_dump.c:10069)
  - [collectComments](../c/collectComments.md) (src/bin/pg_dump/pg_dump.c:10483)
  - getdependencies (src/bin/pg_dump/pg_dump.c:18636, 18651)

## Notes and Other Information
- Returns NULL if catalogIdHash is not initialized or if the CatalogId is not found
- Provides efficient O(1) average-case lookup time using hash table implementation
- Essential for resolving PostgreSQL object references during the dump process
- Forms the foundation for type-specific finder functions (findTableByOid, findTypeByOid, etc.)
- Safe to use with potentially invalid CatalogIds due to built-in NULL checking
- Critical component of pg_dump's object dependency resolution and cross-referencing system