# ginHeapTupleFastCollect

## Location
src/backend/access/gin/ginfast.c: 483 - 553

## Overview
Creates temporary index tuples for a single indexable item from a heap tuple and appends them to a collector array for subsequent bulk insertion into the GIN pending list.

## Definition


## Detailed Description
This function is responsible for converting a single attribute value from a heap tuple into one or more index tuples that will be stored in GIN's pending list. It extracts key values from the input using ginExtractEntries, dynamically manages memory allocation for the collector's tuple array using power-of-2 sizing for efficiency, and creates index tuples for each extracted key. The function protects against integer overflow and ensures the collector has sufficient capacity before adding tuples. Each created index tuple includes the heap tuple's TID for later reference during cleanup operations.

## Parameters / Member Variables
- `ginstate`: Pointer to GinState structure containing index configuration and operator information
- `collector`: Pointer to GinTupleCollector where created tuples will be stored
- `attnum`: Column number of the attribute being indexed
- `value`: The datum value to be indexed
- `isNull`: Boolean indicating whether the value is NULL
- `ht_ctid`: ItemPointer to the heap tuple being indexed

## Dependencies
- Functions called/Symbols referenced:
  - [ginExtractEntries](ginExtractEntries.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - palloc_array
  - repalloc_array
  - [GinFormTuple](../G/GinFormTuple.md)
  - IndexTupleSize
  - MaxAllocSize
- Called from (representative examples):
  - [gininsert](gininsert.md)

## Notes and Other Information
- Part of GIN's fast insertion mechanism for collecting tuples before bulk insertion
- Uses power-of-2 allocation strategy to minimize memory waste during array resizing
- Protects against integer overflow when calculating memory requirements
- Stores heap TID directly in index tuple's t_tid for pending list entries
- Maintains running totals of tuple count and total size in the collector
- Must be followed by ginHeapTupleFastInsert to actually write collected tuples
- Guarantees that all tuples for a single heap tuple are collected together for consistency
- Efficiently handles both initial allocation and dynamic expansion of tuple arrays