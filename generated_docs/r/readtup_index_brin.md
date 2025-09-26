# readtup_index_brin

## Location
[src/backend/utils/sort/tuplesortvariants.c:1755-1784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1755-L1784)

## Overview
Reads and deserializes a BRIN index tuple from a logical tape during external sorting operations, reconstructing the tuple structure and setting up comparison keys.

## Definition
```c
static void readtup_index_brin(Tuplesortstate *state, SortTuple *stup, LogicalTape *tape, unsigned int len)
```

## Detailed Description
This function deserializes a BRIN index tuple that was previously written to a logical tape by writetup_index_brin during external sorting. It reads the tuple data from the tape, allocates appropriate memory for the BrinSortTuple structure, and reconstructs the tuple in memory for further processing.

The function performs several key operations: calculates the actual tuple length by subtracting the length field size, allocates memory for the BrinSortTuple using the tuplesort memory allocation system, reads the tuple data from the tape, handles optional trailing length words for random access, and sets up the datum1 field with the block number for efficient comparison during sorting operations.

The deserialized tuple is integrated into the sorting framework by setting the SortTuple's tuple pointer and datum1 field, making it ready for comparison and merging operations.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure managing the sort operation
- `stup`: Pointer to the SortTuple structure to populate with the deserialized tuple
- `tape`: Pointer to the LogicalTape from which to read the tuple data
- `len`: Total length of the serialized tuple data including the length field

## Dependencies
- Functions called/Symbols referenced:
  - [Tuplesortstate](../T/Tuplesortstate.md) (sort state management structure)
  - SortTuple (generic sort tuple structure)
  - [LogicalTape](../L/LogicalTape.md) (tape abstraction for external sorting)
  - [BrinSortTuple](../B/BrinSortTuple.md) (BRIN-specific tuple structure)
  - TuplesortPublic (public sort state structure)
  - TuplesortstateGetPublic (accessor for public sort state)
  - [tuplesort_readtup_alloc](../t/tuplesort_readtup_alloc.md) (memory allocation for tuple reading)
  - BRINSORTTUPLE_SIZE (macro to calculate BRIN tuple size)
  - LogicalTapeReadExact (function to read exact amount from tape)
  - TUPLESORT_RANDOMACCESS (flag for random access requirement)
- Called from (representative examples):
  - [tuplesort_begin_index_brin](../t/tuplesort_begin_index_brin.md) (BRIN sort initialization)
  - CLUSTER_SORT (clustering sort operations)

## Notes and Other Information
- This function is the counterpart to writetup_index_brin and must handle the same serialization format
- Memory allocation is handled through the tuplesort subsystem for proper memory management
- The trailing length word is conditionally read based on the TUPLESORT_RANDOMACCESS flag
- The datum1 field is set to the block number (bt_blkno) for efficient tuple comparison
- The function integrates seamlessly with PostgreSQL's external sorting infrastructure
- Proper error handling is implicit through LogicalTapeReadExact which will error on insufficient data