# create_range_bounds

## Location
src/backend/partitioning/partbounds.c: 677 - 895

## Overview
Creates a PartitionBoundInfo structure for a range partitioned table by processing boundary specifications and organizing them into a unified, sorted structure.

## Definition


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
  - make_one_partition_rbound
  - qsort_partition_rbound_cmp
  - FunctionCall2Coll
  - datumCopy
  - DatumGetInt32
- Data types used:
  - PartitionBoundInfo
  - PartitionBoundSpec
  - PartitionRangeBound
  - PartitionRangeDatumKind
- Called from:
  - partition_bounds_create
  - compare_range_bounds

## Notes and Other Information
- The function handles default partitions by noting their index but not adding bounds to the processing array
- Bounds are stored with -1 indexes for lower bounds since they represent gaps between partitions
- The final indexes array includes an extra -1 element at the end
- Memory allocation is optimized by allocating single large arrays for datums and kinds rather than many small arrays
- The function ensures all partitions receive canonical indexes from 0 to nparts-1