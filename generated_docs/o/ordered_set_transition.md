# ordered_set_transition

## Location
[src/backend/utils/adt/orderedsetaggs.c:358-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L358-L382)

## Overview
Generic transition function for ordered-set aggregates with a single input column, handling data collection while suppressing null values.

## Definition


## Detailed Description
The  function serves as the state transition function for ordered-set aggregates that operate on a single input column, such as  and . It follows the PostgreSQL aggregate function protocol where the first argument is the aggregate state and subsequent arguments are the input values to be aggregated.

On the first call (when the state argument is null), the function initializes the aggregate state by calling  with  to set up datum-based sorting. For subsequent calls, it retrieves the existing state and adds non-null input values to the tuplesort object using . Null values are automatically filtered out, which is typical behavior for ordered-set aggregates.

The function maintains a count of non-null rows processed, which can be used later by the final function to determine percentiles or other statistics.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments macro containing:
  - Argument 0: Current aggregate state (OSAPerGroupState pointer, initially null)  
  - Argument 1: Input datum value to be added to the sorted collection

## Dependencies
- Functions called/Symbols referenced:
  - [ordered_set_startup](ordered_set_startup.md)
  - [tuplesort_putdatum](../t/tuplesort_putdatum.md)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_DATUM
  - PG_RETURN_POINTER
- Called from (representative examples):
  - PostgreSQL aggregate execution framework for single-column ordered-set aggregates

## Notes and Other Information
- Uses datum-based sorting (not tuple-based) for efficiency with single-column aggregates
- Automatically filters out null input values, which is standard behavior for ordered-set functions
- Maintains running count of non-null rows in 
- Returns the updated state pointer for use by subsequent calls and the final function
- Works with aggregates like 