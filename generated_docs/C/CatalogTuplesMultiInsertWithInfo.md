# CatalogTuplesMultiInsertWithInfo

## Location
[src/backend/catalog/indexing.c:273-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/indexing.c#L273-L312)

## Overview
CatalogTuplesMultiInsertWithInfo efficiently inserts multiple tuples into a system catalog relation in a single operation, optimizing performance by using bulk heap insertion while maintaining individual index updates.

## Definition
```c
void CatalogTuplesMultiInsertWithInfo(Relation heapRel, TupleTableSlot **slot, int ntuples, CatalogIndexState indstate)
```

## Detailed Description
CatalogTuplesMultiInsertWithInfo is designed for high-performance bulk insertion of multiple tuples into system catalog relations. It leverages heap_multi_insert for efficient heap storage while handling catalog indexes individually since there is no multi-insert equivalent for catalog indexes. The function uses pre-opened index state to amortize the cost of index management operations across multiple insertions.

The function handles the conversion from TupleTableSlots to HeapTuples as needed for index insertion, properly managing memory allocation and deallocation. Each tuple's table OID is preserved during the process to maintain proper catalog relationships.

## Parameters / Member Variables
- `heapRel`: The system catalog relation where tuples will be inserted
- `slot`: Array of TupleTableSlot pointers containing the tuples to insert
- `ntuples`: Number of tuples to insert from the slot array
- `indstate`: Pre-opened CatalogIndexState containing information about all indexes on the relation

## Dependencies
- Functions called/Symbols referenced:
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md)
  - [CatalogIndexInsert](CatalogIndexInsert.md)
  - TU_All
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md)
  - [recordMultipleDependencies](../r/recordMultipleDependencies.md)
  - [EnumValuesCreate](../E/EnumValuesCreate.md)
  - [copyTemplateDependencies](../c/copyTemplateDependencies.md)
  - [DefineTSConfiguration](../D/DefineTSConfiguration.md)
  - [MakeConfigurationMapping](../M/MakeConfigurationMapping.md)

## Notes and Other Information
- Returns early if ntuples <= 0, providing safe handling of empty insertion requests
- Uses heap_multi_insert for efficient bulk heap storage with current command ID tracking
- Catalog indexes must be updated individually since PostgreSQL lacks a multi-insert equivalent for catalog indexes
- Properly handles memory management by checking should_free flag and calling heap_freetuple when necessary
- Preserves table OID information (t_tableOid) during HeapTuple conversion for proper catalog relationships
- This function is commonly used in DDL operations that create multiple related catalog entries simultaneously
- The TU_All flag ensures all indexes are updated for each inserted tuple