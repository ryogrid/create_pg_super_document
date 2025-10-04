# CatalogCloseIndexes

## Location
[src/backend/catalog/indexing.c:61-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/indexing.c#L61-L74)

## Overview
CatalogCloseIndexes cleans up and releases resources allocated by CatalogOpenIndexes, properly closing all opened indexes on a system catalog.

## Definition
```c
void CatalogCloseIndexes(CatalogIndexState indstate)
```

## Detailed Description
CatalogCloseIndexes is the cleanup counterpart to CatalogOpenIndexes. It ensures proper resource management by closing all indexes that were opened for a system catalog and freeing the associated CatalogIndexState structure. This function is essential for preventing resource leaks in PostgreSQL's system catalog maintenance operations.

The function performs two key operations: delegating the actual index closing to ExecCloseIndices (which handles the low-level cleanup of index descriptors and related structures) and then freeing the ResultRelInfo structure that was allocated by CatalogOpenIndexes.

## Parameters / Member Variables
- `indstate`: The CatalogIndexState (ResultRelInfo pointer) returned by CatalogOpenIndexes that tracks the opened indexes

## Dependencies
- Functions called/Symbols referenced:
  - [ExecCloseIndices](../E/ExecCloseIndices.md) (closes the actual indexes)
  - [pfree](../p/pfree.md) (deallocates the indstate structure)
- Called from (representative examples):
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [CatalogTupleUpdate](CatalogTupleUpdate.md)
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md)
  - [AddNewAttributeTuples](../A/AddNewAttributeTuples.md)
  - [recordMultipleDependencies](../r/recordMultipleDependencies.md)

## Notes and Other Information
- Must be called for every CatalogIndexState returned by CatalogOpenIndexes to prevent resource leaks
- The indstate parameter becomes invalid after this call and should not be used again
- This function is part of PostgreSQL's internal catalog maintenance infrastructure
- Failure to call this function after CatalogOpenIndexes will result in memory and file descriptor leaks
- Should be called even if errors occur during index operations to ensure proper cleanup

## Simplified Source

```c
void
CatalogCloseIndexes(CatalogIndexState indstate)
{
    // Close all opened indexes and clean up resources
    ExecCloseIndices(indstate);
    pfree(indstate);
}
```