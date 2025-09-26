# readtup_datum

## Location
src/backend/utils/sort/tuplesortvariants.c: 1858 - 1890

## Overview
Reads a datum-based SortTuple from a logical tape during external sorting operations, reconstructing the appropriate datum format and handling NULL values.

## Definition
```c
static void readtup_datum(Tuplesortstate *state, SortTuple *stup, LogicalTape *tape, unsigned int len)
```

## Detailed Description
This function deserializes a datum-based SortTuple from persistent storage via a LogicalTape during external sorting merge operations. It reconstructs the SortTuple by reading the serialized data and handling three cases: NULL values (sets appropriate flags), pass-by-value datums (reads directly into datum1 field), and pass-by-reference datums (allocates memory and reads the pointed-to data). The function uses tuplesort_readtup_alloc() for memory allocation when dealing with variable-length data. It complements writetup_datum() by reversing the serialization process.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing sort configuration and memory management context
- `stup`: Pointer to the SortTuple structure to be populated with the deserialized data
- `tape`: Pointer to the LogicalTape from which the tuple data will be read
- `len`: Total length of the serialized tuple data including the length prefix

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - LogicalTapeReadExact
  - tuplesort_readtup_alloc
  - PointerGetDatum
- Called from (representative examples):
  - tuplesort_begin_datum
  - CLUSTER_SORT

## Notes and Other Information
The function carefully handles memory allocation for pass-by-reference datums using tuplesort_readtup_alloc(), which provides efficient memory management during the merge phase. The tuplen calculation (len - sizeof(unsigned int)) accounts for the length prefix written by writetup_datum(). When TUPLESORT_RANDOMACCESS is enabled, it also reads the trailing length word to maintain tape positioning consistency. This function is essential for the external sorting algorithm that enables PostgreSQL to sort datasets exceeding available memory.