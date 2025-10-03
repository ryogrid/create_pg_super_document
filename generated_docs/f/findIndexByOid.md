# findIndexByOid

## Location
[src/bin/pg_dump/common.c:870-887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L870-L887)

## Overview
Finds and returns the DumpableObject for an index with the specified OID, returning NULL if not found.

## Definition

```c
static IndxInfo *
findIndexByOid(Oid oid)
```
## Detailed Description
This function serves as a specialized lookup utility for finding IndxInfo objects by their database OID. It follows the same pattern as findTableByOid, constructing a CatalogId structure using the provided OID and RelationRelationId, then using the generic findObjectByCatalogId function to locate the corresponding DumpableObject. The function includes an assertion to verify that any found object is indeed an index (DO_INDEX type) before casting and returning it as an IndxInfo pointer.

The function is declared as static, indicating it's used internally within the common.c module. It provides a type-safe way to locate index objects when only the OID is available, which is essential for processing index-related dependencies and relationships during the dump process.

## Parameters / Member Variables
- `oid`: The database OID of the index to find
## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md) (generic object lookup by catalog ID)
  - [CatalogId](../C/CatalogId.md) (structure for identifying catalog objects)
  - DumpableObject (base structure type for dumpable objects)
  - DO_INDEX (enum value for index object type)
  - [IndxInfo](../I/IndxInfo.md) (structure type for index information)
- Called from (representative examples):
  - [flagInhIndexes](flagInhIndexes.md) (in src/bin/pg_dump/common.c:432)
  - Limited usage due to static scope

## Notes and Other Information
- Declared as static, limiting its visibility to the common.c source file
- Returns NULL if no index with the specified OID is found in the dump object registry
- Uses RelationRelationId as the tableoid component, indicating this searches within the pg_class system catalog (indexes are also stored in pg_class)
- Includes a debug assertion that verifies the found object is actually an index type before returning it
- Part of a family of specialized lookup functions that provide type-safe access to specific kinds of database objects
- Less frequently used than findTableByOid due to its static scope and the specific nature of index lookups
- Critical for handling index inheritance and dependency relationships in pg_dump's internal object model