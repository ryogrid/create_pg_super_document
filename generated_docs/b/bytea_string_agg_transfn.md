# bytea_string_agg_transfn

## Location
[src/backend/utils/adt/varlena.c:498-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L498-L550)

## Overview
Transition function for the bytea string_agg() aggregate function that concatenates binary data values with a delimiter during aggregation processing.

## Definition
```c
Datum bytea_string_agg_transfn(PG_FUNCTION_ARGS)
```

## Detailed Description
The bytea_string_agg_transfn function serves as the transition function for PostgreSQL's string_agg() aggregate when operating on bytea (binary data) inputs. This function is called for each input row during aggregation and maintains an internal StringInfo state that accumulates the concatenated binary data.

The function handles parallel aggregation scenarios by carefully managing delimiters. Unlike simple concatenation, it preserves the first delimiter in the StringInfo's cursor field to enable proper joining of partial results from parallel workers. The delimiter is stored but not stripped until the final function processes the complete result.

The function processes three arguments: the current state (StringInfo), the new bytea value to append, and an optional delimiter. If the state is NULL (first call), it initializes a new StringInfo. For non-null values, it appends the delimiter (if provided) followed by the binary data.

## Parameters / Member Variables
- Argument 0: Current aggregation state (StringInfo pointer, may be NULL for first call)
- Argument 1: bytea value to append (may be NULL)  
- Argument 2: bytea delimiter to insert before the value (may be NULL)
- Returns: Updated StringInfo state via `PG_RETURN_POINTER()` or NULL

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_BYTEA_PP
  - [makeStringAggState](../m/makeStringAggState.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - PG_RETURN_POINTER
  - PG_RETURN_NULL
- Called from:
  - (No direct references found - called by PostgreSQL's aggregate function system)

## Notes and Other Information
- Uses the "internal" transition type, which is pass-by-value and pointer-sized
- Designed for parallel aggregation support with careful delimiter handling
- The cursor field of StringInfo stores the length of the first delimiter for later processing
- Handles NULL values gracefully by skipping concatenation when input value is NULL
- Part of PostgreSQL's aggregate function infrastructure for binary data concatenation
- The actual stripping of the first delimiter occurs in the corresponding final function
- Memory management relies on PostgreSQL's memory context system via StringInfo

## Simplified Source

```c
Datum bytea_string_agg_transfn(PG_FUNCTION_ARGS) {
    StringInfo state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

    // Skip if value is null
    if (PG_ARGISNULL(1)) {
        if (state)
            PG_RETURN_POINTER(state);
        PG_RETURN_NULL();
    }

    bytea *value = PG_GETARG_BYTEA_PP(1);
    bool isfirst = false;

    // Initialize state on first call
    if (state == NULL) {
        state = makeStringAggState(fcinfo);
        isfirst = true;
    }

    // Add delimiter if provided
    if (!PG_ARGISNULL(2)) {
        bytea *delim = PG_GETARG_BYTEA_PP(2);
        appendBinaryStringInfo(state, VARDATA_ANY(delim), VARSIZE_ANY_EXHDR(delim));

        // Store first delimiter length for parallel aggregation
        if (isfirst)
            state->cursor = VARSIZE_ANY_EXHDR(delim);
    }

    // Append the value data
    appendBinaryStringInfo(state, VARDATA_ANY(value), VARSIZE_ANY_EXHDR(value));

    PG_RETURN_POINTER(state);
}
```