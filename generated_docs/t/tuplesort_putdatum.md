# tuplesort_putdatum

## Location
src/backend/utils/sort/tuplesortvariants.c: 826 - 889

## Overview
Accepts one Datum value while collecting input data for sorting operations, handling both pass-by-value and pass-by-reference data types with appropriate memory management.

## Definition
```c
void tuplesort_putdatum(Tuplesortstate *state, Datum val, bool isNull)
```

## Detailed Description
This function handles the insertion of individual Datum values into a sorting operation. It distinguishes between pass-by-value and pass-by-reference types, copying pass-by-reference values into controlled memory when necessary. For NULL values and pass-by-value types, the value is stored directly in the sort tuple's datum1 field. For non-null pass-by-reference values, the function creates a copy and sets up both the canonical copy (in tuple field) and potentially an abbreviated value (in datum1 field) for efficient sorting.

## Parameters / Member Variables
- `state`: Tuplesortstate pointer representing the current sorting operation state
- `val`: Datum value to be inserted into the sort
- `isNull`: Boolean flag indicating whether the value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - MemoryContextSwitchTo
  - datumCopy
  - DatumGetPointer
  - tuplesort_puttuple_common
- Called from (representative examples):
  - validate_index_callback
  - ExecEvalAggOrderedTransDatum
  - ExecSort
  - ordered_set_transition

## Notes and Other Information
- Handles NULL values by setting datum1 to zero for consistency and efficient comparison
- Uses abbreviation when available and appropriate for pass-by-reference types
- Memory context switching ensures proper allocation in the tuple context
- Part of the datum-specific sorting infrastructure for single-column sorts
- The canonical copy (stup.tuple) is used for output operations like tuplesort_getdatum