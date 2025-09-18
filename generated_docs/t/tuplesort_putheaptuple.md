# tuplesort_putheaptuple

## Location
src/backend/utils/sort/tuplesortvariants.c: 709 - 751

## Overview
Accepts a HeapTuple and adds it to the tuplesort for sorting, specifically designed for heap tuple sorting operations like table clustering.

## Definition
```c
void tuplesort_putheaptuple(Tuplesortstate *state, HeapTuple tup)
```

## Detailed Description
This function takes a HeapTuple and prepares it for sorting by creating a complete copy of the tuple data and setting up the appropriate sort keys. It is specifically designed for operations that work with heap tuples, such as table clustering operations. The function extracts the first sort key value when available (controlled by haveDatum1 flag) using the index attribute information stored in the sort arguments. It handles memory management by calculating tuple size appropriately for different allocation contexts and supports abbreviation optimization for performance when applicable.

## Parameters / Member Variables
- `state`: The tuplesort state object managing the sort operation
- `tup`: HeapTuple to be added to the sort

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - heap_copytuple
  - heap_getattr
  - TupleSortUseBumpTupleCxt
  - GetMemoryChunkSpace
  - tuplesort_puttuple_common
- Called from (representative examples):
  - heapam_relation_copy_for_cluster

## Notes and Other Information
- Always creates a complete copy of the input HeapTuple data
- Uses TuplesortClusterArg to access index information for key extraction
- Conditionally extracts the first sort key value based on haveDatum1 flag
- Calculates memory usage differently for bump allocation contexts vs. standard contexts
- Supports abbreviation optimization when conditions are met (haveDatum1, abbrev_converter available, and key is not null)
- Primarily used in table clustering operations where heap tuples need to be sorted by index key values
- Memory context management ensures proper allocation in sort-specific contexts