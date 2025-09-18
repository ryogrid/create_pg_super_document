# removeabbrev_heap

## Location
src/backend/utils/sort/tuplesortvariants.c: 1064 - 1084

## Overview
A specialized static function that removes abbreviated keys from heap tuples by extracting the actual attribute values from MinimalTuple structures.

## Definition
```c
static void removeabbrev_heap(Tuplesortstate *state, SortTuple *stups, int count)
```

## Detailed Description
This function is part of PostgreSQL's tuplesort framework, specifically designed for heap tuple sorting operations. When abbreviated keys are being used for performance optimization during sorting, there comes a point where the abbreviations are no longer sufficient and the full attribute values must be retrieved for accurate comparison.

The function operates on an array of SortTuple structures that contain MinimalTuple data. It reconstructs HeapTupleData structures from the minimal tuples and then extracts the actual attribute values using heap_getattr. This process "removes" the abbreviation by replacing the abbreviated datum1 field with the full attribute value.

The function handles the memory layout transformation between MinimalTuple and HeapTuple formats by adjusting pointers and adding the appropriate offset values. This is necessary because MinimalTuple is a space-optimized version of HeapTuple used internally during sorting.

## Parameters / Member Variables
- `state`: Tuplesortstate pointer containing the sort context and configuration information
- `stups`: Array of SortTuple structures containing the tuples to process  
- `count`: Number of tuples in the stups array to process

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [heap_getattr](../h/heap_getattr.md)
  - [HeapTupleData](../H/HeapTupleData.md) (struct type)
  - MinimalTuple (type)
  - HeapTupleHeader (type)
  - MINIMAL_TUPLE_OFFSET (constant)
- Called from (representative examples):
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md) (via CLUSTER_SORT macro)

## Notes and Other Information
- This is a static function, only accessible within the tuplesortvariants.c file
- The function specifically handles the transition from abbreviated to non-abbreviated sorting keys
- It reconstructs HeapTupleData structures from MinimalTuple format by adjusting memory offsets
- The extracted attribute corresponds to the first sort key (sortKeys[0])
- Part of the heap tuple sorting specialization within the broader tuplesort framework
- The CLUSTER_SORT macro references this function, indicating its use in table clustering operations