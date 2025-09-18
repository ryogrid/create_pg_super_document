# BrinSortTuple

## Location
src/backend/utils/sort/tuplesortvariants.c: 157 - 161

## Overview
BrinSortTuple is a wrapper structure used during sorting operations for BRIN (Block Range Index) tuples that includes the tuple length along with the BrinTuple data to facilitate size calculations during sort operations.

## Definition
```c
typedef struct BrinSortTuple
{
    Size        tuplen;
    BrinTuple   tuple;
} BrinSortTuple;
```

## Detailed Description
The BrinSortTuple structure is designed to solve a specific challenge in BRIN index sorting operations. Computing the size of a BrinTuple from the tuple data alone is difficult and computationally expensive. To optimize this process, BrinSortTuple wraps a BrinTuple with its length information, essentially creating a self-describing tuple structure where the size is readily available.

This structure is used internally by the tuple sorting subsystem when dealing with BRIN index tuples. The design allows the sort routines to quickly access the tuple size without having to parse or calculate it from the BrinTuple contents, which improves performance during index creation and maintenance operations.

The structure is specifically used in tuplesortvariants.c as part of PostgreSQL's specialized sorting implementations for different tuple types.

## Parameters / Member Variables
- `tuplen`: Size of the entire BrinSortTuple structure including the BrinTuple data. This provides quick access to the total memory footprint of the structure.
- `tuple`: The actual BrinTuple data containing the BRIN index information for a specific block range, including block number and indexed column data.

## Dependencies
- Functions called/Symbols referenced:
  - [BrinTuple](BrinTuple.md)
- Called from (representative examples):
  - BRINSORTTUPLE_SIZE (macro for size calculation)
  - [tuplesort_putbrintuple](../t/tuplesort_putbrintuple.md)
  - [tuplesort_getbrintuple](../t/tuplesort_getbrintuple.md)
  - [removeabbrev_index_brin](../r/removeabbrev_index_brin.md)
  - [writetup_index_brin](../w/writetup_index_brin.md)
  - [readtup_index_brin](../r/readtup_index_brin.md)

## Notes and Other Information
- The structure is specifically designed for internal use within the tuple sorting subsystem and is not exposed to external callers
- The BRINSORTTUPLE_SIZE macro is provided to calculate the total size of a BrinSortTuple given the length of the underlying BrinTuple
- This design pattern of prefixing data structures with size information is common in PostgreSQL's internal APIs where performance-critical operations need quick access to object sizes
- The structure is only used during sorting operations and is not persisted to disk - the underlying BrinTuple is what gets stored in the actual BRIN index