# writetup_heap

## Location
[src/backend/utils/sort/tuplestore.c:1500-1520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L1500-L1520)

## Overview
Writes a minimal tuple from a heap sort operation to a logical tape during the external sort phase of tuple sorting.

## Definition

```c
static void
writetup_heap(Tuplestorestate *state, void *tup)
```
## Detailed Description
This function is responsible for serializing and writing heap tuples to a logical tape during external sorting operations. It extracts the minimal tuple data from a SortTuple structure, calculates the appropriate lengths, and writes the tuple data to the specified logical tape. The function handles both normal writing and random access scenarios where trailing length information is required.

The function operates by:
1. Extracting the MinimalTuple from the SortTuple
2. Calculating the tuple body length (excluding the minimal tuple header)
3. Writing the total tuple length as a header
4. Writing the actual tuple body data
5. Optionally writing a trailing length word for random access support

## Parameters / Member Variables
- : The tuplesort state containing sorting configuration and context information
- : The logical tape to write the tuple data to
- : The SortTuple containing the heap tuple to be written

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [LogicalTapeWrite](../L/LogicalTapeWrite.md)
- Data structures used:
  - TuplesortPublic
  - MinimalTuple
  - [LogicalTape](../L/LogicalTape.md)
  - SortTuple
- Constants referenced:
  - MINIMAL_TUPLE_DATA_OFFSET
  - TUPLESORT_RANDOMACCESS
- Called from (representative examples):
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md)
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)

## Notes and Other Information
- This is a static function specific to heap tuple sorting operations
- The function writes tuple data in a format compatible with the corresponding readtup_heap function
- Random access support adds trailing length information when TUPLESORT_RANDOMACCESS option is enabled
- The function only writes the tuple body data, excluding the minimal tuple header to optimize storage
- Part of PostgreSQL's external sorting infrastructure used for large dataset operations that exceed memory capacity