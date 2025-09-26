# removeabbrev_cluster

## Location
[src/backend/utils/sort/tuplesortvariants.c:1208-1226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1208-L1226)

## Overview
Removes abbreviation optimization for cluster sorting by extracting and caching the first sort key attribute from each HeapTuple for direct comparison operations.

## Definition
```c
static void removeabbrev_cluster(Tuplesortstate *state, SortTuple *stups, int count)
```

## Detailed Description
The `removeabbrev_cluster` function is called when the tuple sorting system decides to abandon abbreviation optimization during CLUSTER operations. Abbreviation is a performance optimization where shorter representations of sort keys are used for faster comparisons. When this optimization proves ineffective (typically due to low cardinality or poor distribution), the system falls back to full key comparisons.

This function iterates through an array of SortTuple structures and extracts the first sort key attribute from each HeapTuple using the index definition specified in the TuplesortClusterArg. The extracted attribute values are cached in the `datum1` field of each SortTuple for subsequent comparison operations.

The function is specifically designed for CLUSTER operations, which sort table data according to a btree index definition to physically reorganize the table storage.

## Parameters / Member Variables
- `state`: Tuplesortstate containing the sorting context and configuration
- `stups`: Array of SortTuple structures to process 
- `count`: Number of SortTuple entries in the stups array

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [heap_getattr](../h/heap_getattr.md)
  - TuplesortClusterArg
  - TuplesortPublic
- Called from (representative examples):
  - [tuplesort_begin_cluster](../t/tuplesort_begin_cluster.md)
  - CLUSTER_SORT operations

## Notes and Other Information
- This function is part of the abbreviation removal mechanism in PostgreSQL's tuple sorting infrastructure
- The first index attribute (ii_IndexAttrNumbers[0]) is used as the primary sort key for clustering operations
- The extracted attribute values are stored in `datum1` and null status in `isnull1` for efficient access during comparisons
- CLUSTER operations use btree index definitions to determine the sort order for physically reorganizing table data
- This fallback mechanism ensures correct sorting behavior even when abbreviation optimization fails to provide performance benefits