# tuplesort_begin_datum

## Location
src/backend/utils/sort/tuplesortvariants.c: 584 - 668

## Overview
Initializes a tuplesort state for sorting individual datum values of a specific data type, supporting efficient sorting of single-column data with comprehensive type-aware optimizations.

## Definition
```c
Tuplesortstate *tuplesort_begin_datum(Oid datumType, Oid sortOperator, Oid sortCollation, bool nullsFirstFlag, int workMem, SortCoordinate coordinate, int sortopt)
```

## Detailed Description
This function creates a specialized tuplesort state for sorting raw datum values rather than complete tuples. It configures type-specific sorting behavior including collation handling, null ordering, and performance optimizations like abbreviation for pass-by-reference types. The function sets up SortSupport infrastructure to leverage type-specific comparison functions and enables the "onlyKey" optimization when abbreviation is not used. This variant is particularly useful for aggregate functions, ordered set aggregates, and index validation operations that need to sort single values efficiently.

## Parameters / Member Variables
- `datumType`: OID of the data type being sorted
- `sortOperator`: OID of the comparison operator to use for sorting
- `sortCollation`: OID of the collation to use for text-based comparisons
- `nullsFirstFlag`: Whether NULL values should sort before non-NULL values
- `workMem`: Amount of work memory (in kilobytes) available for the sort operation
- `coordinate`: Shared state for coordinating parallel sorts (can be NULL for non-parallel sorts)
- `sortopt`: Bitwise flags controlling sort behavior (e.g., TUPLESORT_RANDOMACCESS)

## Dependencies
- Functions called/Symbols referenced:
  - tuplesort_begin_common
  - TuplesortstateGetPublic
  - get_typlenbyval
  - PrepareSortSupportFromOrderingOp
  - removeabbrev_datum, comparetup_datum, comparetup_datum_tiebreak
  - writetup_datum, readtup_datum
- Called from (representative examples):
  - validate_index
  - initialize_aggregate
  - ExecSort
  - ordered_set_startup

## Notes and Other Information
- Always configured as single-column sort (nKeys = 1)
- Enables abbreviation optimization only for pass-by-reference types to avoid data loss
- Uses "onlyKey" optimization when abbreviation is not available for better performance
- Configures tuples field based on whether the type is pass-by-value or pass-by-reference
- Supports comprehensive type system integration through SortSupport infrastructure
- Includes DTrace/tracing support for performance monitoring when enabled