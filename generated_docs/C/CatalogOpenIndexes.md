# CatalogOpenIndexes

## Location
[src/backend/catalog/indexing.c:43-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/indexing.c#L43-L60)

## Overview
CatalogOpenIndexes opens all indexes on a system catalog relation to prepare for tuple insertion or update operations, returning a CatalogIndexState for managing index operations.

## Definition
```c
CatalogIndexState CatalogOpenIndexes(Relation heapRel)
```

## Detailed Description
CatalogOpenIndexes is a specialized function for opening indexes on PostgreSQL system catalogs. It creates and initializes a ResultRelInfo structure to manage index operations without the overhead of creating a full EState execution context. This design choice means the function supports basic index operations but does not support partial indexes, expressional indexes, or generalized exclusion constraints on system catalogs.

The function leverages shared infrastructure with execUtils.c for opening and closing indexes, but uses a simplified approach optimized for system catalog operations. It creates a dummy ResultRelInfo node with minimal initialization and delegates the actual index opening to ExecOpenIndices.

## Parameters / Member Variables
- `heapRel`: The system catalog relation (table) for which indexes should be opened

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates ResultRelInfo node)
  - [ExecOpenIndices](../E/ExecOpenIndices.md) (opens the actual indexes)
- Called from (representative examples):
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [CatalogTupleUpdate](CatalogTupleUpdate.md)
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md)
  - [AddNewAttributeTuples](../A/AddNewAttributeTuples.md)
  - [recordMultipleDependencies](../r/recordMultipleDependencies.md)

## Notes and Other Information
- Returns a CatalogIndexState (which is actually a ResultRelInfo pointer) that must be closed with CatalogCloseIndexes
- Does not support partial or expressional indexes on system catalogs due to the simplified EState-free implementation
- The ri_RangeTableIndex is set to 0 as a dummy value since system catalog operations do not use range tables
- Triggers are disabled (ri_TrigDesc = NULL) for system catalog index operations
- This is part of PostgreSQL's internal catalog maintenance infrastructure and should not be used for user table operations