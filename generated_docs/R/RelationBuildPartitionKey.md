# RelationBuildPartitionKey

## Location
[src/backend/utils/cache/partcache.c:78-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/partcache.c#L78-L276)

## Overview
Constructs and caches the complete partition key data structure for a partitioned table by reading metadata from system catalogs and building all necessary type and operator information.

## Definition
```c
static void RelationBuildPartitionKey(Relation relation)
```

## Detailed Description
RelationBuildPartitionKey is a complex internal function that builds the complete PartitionKey structure for a partitioned table. The function retrieves partition metadata from the pg_partitioned_table catalog and constructs a comprehensive data structure containing all information needed for partitioning operations.

The function implements careful memory management by creating a dedicated memory context ("partition key") as a child of CurTransactionContext initially, then reparenting it to CacheMemoryContext only after successful completion. This prevents memory leaks if errors occur during construction.

Key operations include:
- Reading partition strategy, attribute count, and attribute numbers from pg_partitioned_table
- Processing operator classes and collations for each partition attribute  
- Parsing and optimizing partition expressions (for expression-based partitioning)
- Looking up support functions for comparison/hashing operations
- Collecting complete type information for each partition column
- Validating partition strategy and operator class compatibility

## Parameters / Member Variables
- `relation`: The partitioned table relation for which to build the partition key

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (catalog lookup)
  - AllocSetContextCreate (memory context creation)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md) (retrieving operator classes and collations)
  - [stringToNode](../s/stringToNode.md) (parsing partition expressions)
  - [eval_const_expressions](../e/eval_const_expressions.md) (optimizing expressions)
  - [fix_opfuncids](../f/fix_opfuncids.md) (fixing operator function IDs)
  - [get_opfamily_proc](../g/get_opfamily_proc.md) (finding support functions)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (caching function information)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md) (type information)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md) (memory management)
- Called from:
  - [RelationGetPartitionKey](RelationGetPartitionKey.md) (when rd_partkey is NULL)

## Notes and Other Information
- Function is static (internal to partcache.c)
- Creates dedicated memory context to avoid complex cleanup logic
- Supports all partition strategies: LIST, RANGE, and HASH
- Handles both column-based and expression-based partitioning
- Validates operator class support for the chosen partition strategy
- Uses different support function numbers based on strategy (HASHEXTENDED_PROC for hash, BTORDER_PROC for others)
- Performs const-simplification on partition expressions for planner compatibility
- Memory context is initially created under CurTransactionContext and only reparented to CacheMemoryContext on success
- Results are cached in relation->rd_partkey and persist until relation is closed