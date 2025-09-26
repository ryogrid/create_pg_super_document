# mode_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:1033-1141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L1033-L1141)

## Overview
Implements the final phase of the PostgreSQL aggregate function `mode() within group (anyelement)`, which finds the most frequently occurring value in a dataset.

## Definition
```c
Datum mode_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the finalization function for the `mode` ordered-set aggregate that computes the statistical mode (most common value) of a dataset. The function processes a sorted dataset by scanning through all values, counting frequencies of each distinct value, and tracking which value appears most frequently.

The algorithm maintains state for both the current mode candidate and the last processed value to efficiently count consecutive identical values. It uses abbreviated keys optimization when available to avoid expensive equality function calls. The function handles memory management appropriately for pass-by-reference data types.

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure containing the aggregate state

## Dependencies
- Functions called/Symbols referenced:
  - [OSAPerGroupState](../O/OSAPerGroupState.md): Ordered-set aggregate per-group state structure
  - [AggCheckCallContext](../A/AggCheckCallContext.md): Validates aggregate calling context
  - [get_opcode](../g/get_opcode.md): Retrieves procedure OID for equality operator
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md): Initializes function call information
  - `[tuplesort_performsort](../t/tuplesort_performsort.md)`: Completes the sorting operation
  - `[tuplesort_rescan](../t/tuplesort_rescan.md)`: Resets tuple sort for reading
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md): Retrieves next datum from sorted data
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md): Calls equality function with collation
  - `PG_GET_COLLATION`: Gets collation from function call info
- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct callers found in indexed code)

## Notes and Other Information
- This is part of PostgreSQL's ordered-set aggregate implementation for statistical functions
- The function is registered as an aggregate final function in the system catalogs
- Efficiently handles large datasets by using abbreviated keys to minimize equality comparisons
- Properly manages memory for pass-by-reference data types to prevent leaks
- Returns NULL if no non-null values were found in the input dataset
- Uses `CHECK_FOR_INTERRUPTS()` to allow query cancellation during long operations
- The algorithm is optimized to work with the sorted input from the ordered-set aggregate framework