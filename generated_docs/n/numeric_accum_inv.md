# numeric_accum_inv

## Location
[src/backend/utils/adt/numeric.c:5447-5482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5447-L5482)

## Overview
Generic inverse transition function for numeric aggregates that removes a value from the aggregate state, supporting both simple and complex aggregates with or without sumX2 requirements.

## Definition
```c
Datum numeric_accum_inv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the inverse transition for numeric aggregates, allowing values to be removed from an existing aggregate state. It's a critical component for supporting moving window aggregates and other scenarios where aggregate states need to be decrementally updated.

The function works with both simple aggregates (that only need sumX) and complex statistical aggregates (that require sumX2 for variance/stddev calculations). It delegates the actual removal logic to do_numeric_discard, which handles the mathematical operations needed to subtract a value's contribution from the aggregate state.

If the inverse operation fails (which can happen due to numerical precision issues or invalid state transitions), the function returns NULL to indicate that the aggregate cannot be reliably computed using the incremental approach, forcing a full recalculation.

## Parameters / Member Variables
- `fcinfo`: Function call information containing:
  - Argument 0: NumericAggState pointer (current aggregate state)  
  - Argument 1: Numeric value to remove from the aggregate

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL: Checks if function arguments are NULL
  - PG_GETARG_POINTER: Retrieves the NumericAggState from argument 0
  - PG_GETARG_NUMERIC: Retrieves the numeric value from argument 1  
  - [do_numeric_discard](../d/do_numeric_discard.md): Performs the actual inverse accumulation operation
  - PG_RETURN_NULL: Returns NULL if inverse operation fails
  - PG_RETURN_POINTER: Returns the updated state pointer
- Called from (representative examples):
  - Not directly referenced by other symbols (used by aggregate framework)

## Notes and Other Information
- Generic function working with both simple and complex numeric aggregates
- Essential for moving window aggregates and decremental aggregate updates
- Returns NULL if inverse operation cannot be performed reliably
- Part of PostgreSQL's advanced aggregation system supporting efficient window functions
- Handles NULL input values gracefully by skipping the removal operation
- Uses do_numeric_discard for the mathematical heavy lifting of state modification

## Simplified Source

```c
Datum numeric_accum_inv(PG_FUNCTION_ARGS) {
    NumericAggState *state;

    // Get aggregate state from first argument
    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    // Validate state exists
    if (state == NULL)
        elog(ERROR, "numeric_accum_inv called with NULL state");

    // Remove value from aggregate if not NULL
    if (!PG_ARGISNULL(1)) {
        // Attempt inverse operation - return NULL if it fails
        if (!do_numeric_discard(state, PG_GETARG_NUMERIC(1)))
            PG_RETURN_NULL();
    }

    PG_RETURN_POINTER(state);
}
```