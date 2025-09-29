# tuplesort_begin_datum

## Location
[src/backend/utils/sort/tuplesortvariants.c:584-668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L584-L668)

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
  - [tuplesort_begin_common](tuplesort_begin_common.md)
  - TuplesortstateGetPublic
  - [get_typlenbyval](../g/get_typlenbyval.md)
  - [PrepareSortSupportFromOrderingOp](../P/PrepareSortSupportFromOrderingOp.md)
  - [removeabbrev_datum](../r/removeabbrev_datum.md), comparetup_datum, comparetup_datum_tiebreak
  - [writetup_datum](../w/writetup_datum.md), readtup_datum
- Called from (representative examples):
  - [validate_index](../v/validate_index.md)
  - [initialize_aggregate](../i/initialize_aggregate.md)
  - [ExecSort](../E/ExecSort.md)
  - [ordered_set_startup](../o/ordered_set_startup.md)

## Notes and Other Information
- Always configured as single-column sort (nKeys = 1)
- Enables abbreviation optimization only for pass-by-reference types to avoid data loss
- Uses "onlyKey" optimization when abbreviation is not available for better performance
- Configures tuples field based on whether the type is pass-by-value or pass-by-reference
- Supports comprehensive type system integration through SortSupport infrastructure
- Includes DTrace/tracing support for performance monitoring when enabled

## Simplified Source

```c
Tuplesortstate *tuplesort_begin_datum(Oid datumType, Oid sortOperator, Oid sortCollation,
                                      bool nullsFirstFlag, int workMem,
                                      SortCoordinate coordinate, int sortopt) {
    // Initialize common tuplesort state
    Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate, sortopt);
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    TuplesortDatumArg *arg;

    // Switch to sort memory context and allocate argument structure
    MemoryContext oldcontext = MemoryContextSwitchTo(base->maincontext);
    arg = (TuplesortDatumArg *) palloc(sizeof(TuplesortDatumArg));

    // Configure as single-column sort
    base->nKeys = 1;

    // Set up datum-specific function pointers
    base->comparetup = comparetup_datum;
    base->writetup = writetup_datum;
    base->readtup = readtup_datum;
    base->haveDatum1 = true;
    base->arg = arg;

    // Get type properties and configure based on pass-by-value vs pass-by-reference
    int16 typlen;
    bool typbyval;
    get_typlenbyval(datumType, &typlen, &typbyval);

    arg->datumType = datumType;
    arg->datumTypeLen = typlen;
    base->tuples = !typbyval;  // Store tuples for pass-by-reference types

    // Set up sort support with collation and null handling
    base->sortKeys = (SortSupport) palloc0(sizeof(SortSupportData));
    base->sortKeys->ssup_cxt = CurrentMemoryContext;
    base->sortKeys->ssup_collation = sortCollation;
    base->sortKeys->ssup_nulls_first = nullsFirstFlag;

    // Enable abbreviation only for pass-by-reference types
    base->sortKeys->abbreviate = !typbyval;

    // Configure sort operator and enable onlyKey optimization if no abbreviation
    PrepareSortSupportFromOrderingOp(sortOperator, base->sortKeys);
    if (!base->sortKeys->abbrev_converter)
        base->onlyKey = base->sortKeys;

    MemoryContextSwitchTo(oldcontext);
    return state;
}
```