# writetup_index_brin

## Location
[src/backend/utils/sort/tuplesortvariants.c:1741-1754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1741-L1754)

## Overview
Writes a BRIN index tuple to a logical tape during external sorting operations, handling the serialization format and optional random access requirements.

## Definition
```c
static void writetup_index_brin(Tuplesortstate *state, LogicalTape *tape, SortTuple *stup)
```

## Detailed Description
This function serializes a BRIN index tuple to a logical tape as part of PostgreSQL's external sorting mechanism. When sorting operations exceed available memory, tuples are written to temporary tape files for later merging. The function writes the tuple data in a specific format that includes the tuple length followed by the actual tuple data.

The serialization format consists of: a length word (tuple length + sizeof(length)), the actual BRIN tuple data, and optionally a trailing length word if random access is required. The trailing length word enables backward traversal of the tape when the TUPLESORT_RANDOMACCESS option is set, which is useful for certain sorting algorithms that need to read tuples in reverse order.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure managing the sort operation
- `tape`: Pointer to the LogicalTape where the tuple will be written
- `stup`: Pointer to the SortTuple containing the BRIN tuple to serialize

## Dependencies
- Functions called/Symbols referenced:
  - Tuplesortstate (sort state management structure)
  - [LogicalTape](../L/LogicalTape.md) (tape abstraction for external sorting)
  - SortTuple (generic sort tuple structure)
  - TuplesortPublic (public sort state structure)
  - TuplesortstateGetPublic (accessor for public sort state)
  - [BrinSortTuple](../B/BrinSortTuple.md) (BRIN-specific tuple structure)
  - LogicalTapeWrite (function to write data to logical tape)
  - TUPLESORT_RANDOMACCESS (flag for random access requirement)
- Called from (representative examples):
  - [tuplesort_begin_index_brin](../t/tuplesort_begin_index_brin.md) (BRIN sort initialization)
  - CLUSTER_SORT (clustering sort operations)

## Notes and Other Information
- The function writes tuples in a self-describing format with length prefixes for proper deserialization
- The trailing length word is conditionally written based on the TUPLESORT_RANDOMACCESS flag
- This serialization format must be compatible with the corresponding readtup_index_brin function
- The tuplen field includes the size of the length word itself in the total length calculation
- External sorting is used when the dataset is too large to fit entirely in memory
- The logical tape abstraction allows PostgreSQL to handle very large sorts efficiently