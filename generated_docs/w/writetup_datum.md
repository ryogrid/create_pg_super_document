# writetup_datum

## Location
src/backend/utils/sort/tuplesortvariants.c: 1824 - 1857

## Overview
Writes a datum-based SortTuple to a logical tape during external sorting operations, handling both NULL values and different datum storage formats.

## Definition
```c
static void writetup_datum(Tuplesortstate *state, LogicalTape *tape, SortTuple *stup)
```

## Detailed Description
This function serializes a datum-based SortTuple to persistent storage via a LogicalTape during external sorting when memory is insufficient to hold all tuples. It handles three different cases: NULL values (writes no data), pass-by-value datums (writes the Datum directly), and pass-by-reference datums (writes the pointed-to data). The function writes a length prefix before the data and optionally a trailing length suffix for random access support. The serialization format depends on whether the base->tuples field is set, which indicates pass-by-reference storage.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing sort configuration and datum type information
- `tape`: Pointer to the LogicalTape where the tuple data will be written
- `stup`: Pointer to the SortTuple containing the datum to be written

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - datumGetSize
  - PointerGetDatum
  - LogicalTapeWrite
- Called from (representative examples):
  - tuplesort_begin_datum
  - CLUSTER_SORT

## Notes and Other Information
The function implements a space-efficient serialization format by only writing the actual data size rather than a fixed-size buffer. For pass-by-reference datums, it uses datumGetSize() to determine the correct number of bytes to write. The trailing length word is only written when TUPLESORT_RANDOMACCESS option is enabled, allowing backward traversal of the tape. This function is part of the external sorting mechanism that allows PostgreSQL to sort datasets larger than available memory.