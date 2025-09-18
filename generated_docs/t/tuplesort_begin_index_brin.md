# tuplesort_begin_index_brin

## Location
[src/backend/utils/sort/tuplesortvariants.c:555-583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L555-L583)

## Overview
Initializes a tuplesort state for sorting BRIN (Block Range Index) index tuples, specifically designed to sort index entries by block number.

## Definition


## Detailed Description
This function creates and configures a tuplesort state specifically optimized for BRIN index operations. BRIN indexes work with block ranges, so this variant is designed to sort index tuples primarily by block number. The function sets up specialized comparison, read/write, and abbreviation removal functions that are tailored for BRIN index tuple handling. It configures the sort to use only one sort column (the block number) and enables datum-based sorting optimizations.

## Parameters / Member Variables
- : Amount of work memory (in kilobytes) available for the sort operation
- : Shared state for coordinating parallel sorts (can be NULL for non-parallel sorts)
- : Bitwise flags controlling sort behavior (e.g., TUPLESORT_RANDOMACCESS for random access capability)

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_common](tuplesort_begin_common.md)
  - TuplesortstateGetPublic
  - [removeabbrev_index_brin](../r/removeabbrev_index_brin.md)
  - [comparetup_index_brin](../c/comparetup_index_brin.md)
  - [writetup_index_brin](../w/writetup_index_brin.md)
  - [readtup_index_brin](../r/readtup_index_brin.md)
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md)
  - [_brin_parallel_scan_and_build](../b/_brin_parallel_scan_and_build.md)

## Notes and Other Information
- Configures nKeys = 1 since BRIN indexes primarily sort by block number only
- Sets haveDatum1 = true to enable datum-based sorting optimizations
- Uses specialized BRIN-specific tuple handling functions for optimal performance
- Part of the tuplesort framework's extensible design for different index types
- Enables trace logging when TRACE_SORT is defined for debugging purposes