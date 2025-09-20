# create_range_bounds

## Location
[src/backend/partitioning/partbounds.c:677-895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L677-L895)

## Overview
Creates a PartitionBoundInfo structure for a range partitioned table by processing boundary specifications and organizing them into a unified, sorted structure.

## Definition

```c
static PartitionBoundInfo
create_range_bounds(PartitionBoundSpec **boundspecs, int nparts,
					PartitionKey key, int **mapping)
```
## Detailed Description
This function takes an array of partition boundary specifications for range partitions and creates a unified PartitionBoundInfo structure. It processes both lower and upper bounds from all partitions, sorts them, removes duplicates, and creates the final boundary structure with proper indexing. The function handles default partitions specially and assigns canonical indexes to each partition.

The function creates a comprehensive boundary structure by:
1. Extracting both lower and upper bounds from each partition specification
2. Creating a unified list of all bounds across partitions
3. Sorting bounds in ascending order using partition-specific comparison
4. Removing duplicate bounds to create a distinct set
5. Building the final PartitionBoundInfo with proper indexing

## Parameters / Member Variables
- : Array of PartitionBoundSpec pointers containing the boundary specifications for each partition
- : Number of partitions being processed
- : PartitionKey containing partitioning metadata (comparison functions, data types, etc.)
- : Pointer to mapping array that will be updated to map original partition indexes to canonical indexes

## Dependencies
- Functions called/Symbols referenced:
  - [make_one_partition_rbound](../m/make_one_partition_rbound.md)
  - [qsort_partition_rbound_cmp](../q/qsort_partition_rbound_cmp.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [datumCopy](../d/datumCopy.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
- Data types used:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - [PartitionBoundSpec](../P/PartitionBoundSpec.md)
  - [PartitionRangeBound](../P/PartitionRangeBound.md)
  - [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md)
- Called from:
  - [partition_bounds_create](../p/partition_bounds_create.md)
  - compare_range_bounds

## Notes and Other Information
- The function handles default partitions by noting their index but not adding bounds to the processing array
- Bounds are stored with -1 indexes for lower bounds since they represent gaps between partitions
- The final indexes array includes an extra -1 element at the end
- Memory allocation is optimized by allocating single large arrays for datums and kinds rather than many small arrays
- The function ensures all partitions receive canonical indexes from 0 to nparts-1