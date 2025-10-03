# tuplesort_getdatum

## Location
[src/backend/utils/sort/tuplesortvariants.c:1018-1063](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1018-L1063)

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
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [tuplesort_gettuple_common](tuplesort_gettuple_common.md)
  - [datumCopy](../d/datumCopy.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - TuplesortDatumArg (struct type)
  - SortTuple (struct type)
- Called from (representative examples):
  - [heapam_index_validate_scan](../h/heapam_index_validate_scan.md)
  - [process_ordered_aggregate_single](../p/process_ordered_aggregate_single.md)
  - [ExecSort](../E/ExecSort.md)
  - [percentile_disc_final](../p/percentile_disc_final.md)
  - [percentile_cont_final_common](../p/percentile_cont_final_common.md)
  - [mode_final](../m/mode_final.md)

## Notes and Other Information
- Returns false when no more datums are available in the specified direction
- For pass-by-ref types with copy=true, the returned value is freshly allocated in the caller's context
- For pass-by-ref types with copy=false, the returned pointer becomes invalid after subsequent tuplesort manipulations
- The copy parameter has no effect for pass-by-value datums
- Abbreviated keys can be used for efficient inequality comparisons without full datum evaluation
- NULL values have zeroed abbreviated key representations
- The function is extensively used in aggregate operations and sorting contexts throughout PostgreSQL

## Simplified Source

```c
bool
tuplesort_getdatum(Tuplesortstate *state, bool forward, bool copy,
                   Datum *val, bool *isNull, Datum *abbrev)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    MemoryContext oldcontext = MemoryContextSwitchTo(base->sortcontext);
    TuplesortDatumArg *arg = (TuplesortDatumArg *) base->arg;
    SortTuple stup;

    // Get next tuple from common retrieval function
    if (!tuplesort_gettuple_common(state, forward, &stup)) {
        MemoryContextSwitchTo(oldcontext);
        return false;
    }

    // Switch back to caller's memory context
    MemoryContextSwitchTo(oldcontext);

    // Set abbreviated key if requested
    if (base->sortKeys->abbrev_converter && abbrev)
        *abbrev = stup.datum1;

    // Handle null values or non-tuple case
    if (stup.isnull1 || !base->tuples) {
        *val = stup.datum1;
        *isNull = stup.isnull1;
    } else {
        // Handle pass-by-ref datums with copy control
        if (copy)
            *val = datumCopy(PointerGetDatum(stup.tuple), false,
                             arg->datumTypeLen);
        else
            *val = PointerGetDatum(stup.tuple);
        *isNull = false;
    }

    return true;
}
```