# tuplesort_getdatum

## Location
src/backend/utils/sort/tuplesortvariants.c: 1018 - 1063

## Overview
Fetches the next Datum from a tuplesort state in either forward or backward direction, providing flexible memory management options for both pass-by-value and pass-by-reference data types.

## Definition
```c
bool tuplesort_getdatum(Tuplesortstate *state, bool forward, bool copy, Datum *val, bool *isNull, Datum *abbrev)
```

## Detailed Description
This function is a critical component of PostgreSQL's tuplesort framework, specifically designed for retrieving sorted Datum values. Unlike similar functions for other tuple types, it provides sophisticated memory management capabilities that are essential for handling both pass-by-value and pass-by-reference data types safely.

The function handles abbreviated keys when available, which can provide performance benefits by allowing callers to perform cheap inequality comparisons without full datum comparisons. For pass-by-reference types, it offers two modes of operation: copying the datum into the caller's memory context for safe long-term use, or providing a pointer to the tuplesort's internal storage for more efficient short-term access.

The function manages memory contexts carefully, switching to the sort context for internal operations and back to the caller's context for result preparation, ensuring proper memory lifecycle management.

## Parameters / Member Variables
- `state`: Tuplesortstate pointer representing the active tuplesort operation containing datum values
- `forward`: Boolean flag indicating retrieval direction (true for forward, false for backward)
- `copy`: Boolean flag controlling memory management for pass-by-ref datums (true copies to caller's context, false provides internal pointer)
- `val`: Output parameter that receives the retrieved Datum value
- `isNull`: Output parameter that receives the null status of the retrieved datum
- `abbrev`: Optional output parameter that receives the abbreviated key value when abbreviation is used

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - MemoryContextSwitchTo
  - tuplesort_gettuple_common
  - datumCopy
  - PointerGetDatum
  - TuplesortDatumArg (struct type)
  - SortTuple (struct type)
- Called from (representative examples):
  - heapam_index_validate_scan
  - process_ordered_aggregate_single
  - ExecSort
  - percentile_disc_final
  - percentile_cont_final_common
  - mode_final

## Notes and Other Information
- Returns false when no more datums are available in the specified direction
- For pass-by-ref types with copy=true, the returned value is freshly allocated in the caller's context
- For pass-by-ref types with copy=false, the returned pointer becomes invalid after subsequent tuplesort manipulations
- The copy parameter has no effect for pass-by-value datums
- Abbreviated keys can be used for efficient inequality comparisons without full datum evaluation
- NULL values have zeroed abbreviated key representations
- The function is extensively used in aggregate operations and sorting contexts throughout PostgreSQL