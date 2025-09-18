# readtup_cluster

## Location
src/backend/utils/sort/tuplesortvariants.c: 1370 - 1398

## Overview
Reads a heap tuple from a logical tape during CLUSTER sort operations, reconstructing the HeapTuple structure and extracting key values for sorting comparison.

## Definition


## Detailed Description
The `readtup_cluster` function is the counterpart to `writetup_cluster`, responsible for deserializing heap tuples from logical tapes during CLUSTER operations. It reconstructs a complete HeapTuple structure from the serialized data, including the tuple header, data, and physical location information.

The function performs the following operations:
1. Allocates memory for the HeapTuple structure using `tuplesort_readtup_alloc`
2. Reconstructs the HeapTupleData header with appropriate pointers
3. Reads the ItemPointer (t_self) containing the tuple's physical location
4. Reads the actual tuple data from the tape
5. Extracts the first-column key value for efficient sorting comparisons (if applicable)

This function is essential for the CLUSTER operation as it maintains the relationship between the sorted tuple data and the original physical storage locations, which is required for the clustering process.

## Parameters / Member Variables
- `state`: The tuplesort state containing configuration and context information
- `stup`: The SortTuple structure to populate with the reconstructed tuple
- `tape`: The logical tape to read the tuple data from
- `tuplen`: The total length of the serialized tuple data

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - tuplesort_readtup_alloc
  - LogicalTapeReadExact
  - heap_getattr
  - TUPLESORT_RANDOMACCESS (flag)
  - HEAPTUPLESIZE (constant)
- Called from (representative examples):
  - CLUSTER_SORT operations
  - tuplesort_begin_cluster

## Notes and Other Information
- This function is the read counterpart to `writetup_cluster` and must handle the same serialization format
- The function reconstructs the HeapTupleData header but sets t_tableOid to InvalidOid as it's not currently stored/needed
- Memory allocation is handled by `tuplesort_readtup_alloc` which provides efficient memory management for tuple sorting
- The first-column key value extraction (`datum1`) is performed for optimization, allowing faster comparisons during sorting
- The trailing length word is read conditionally based on the TUPLESORT_RANDOMACCESS flag for backward tape navigation
- The function assumes the tape data is valid and in the expected format written by `writetup_cluster`