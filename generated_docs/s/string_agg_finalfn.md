# string_agg_finalfn

## Location
[src/backend/utils/adt/varlena.c:5358-5383](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5358-L5383)

## Overview
The final function for PostgreSQL's string_agg() aggregate that produces the final text result by removing the leading delimiter from the accumulated string.

## Definition


## Detailed Description
The string_agg_finalfn function serves as the final function for the string_agg aggregate, responsible for producing the ultimate text result. Its primary purpose is to remove the leading delimiter that was preserved during the transition and combine phases for parallel aggregation support.

Key behavioral aspects:
- Takes the accumulated StringInfo state as input
- Strips off the leading delimiter using the cursor position stored by string_agg_transfn
- Returns NULL if the input state is NULL (no non-NULL values were aggregated)
- Converts the processed C string to a PostgreSQL text datum
- The cursor field indicates how many bytes to skip from the beginning of the accumulated data

The function completes the string_agg operation by:
1. Checking if there's any accumulated state
2. Using the cursor position to skip the first delimiter
3. Creating a text datum from the remaining data

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing:
  - Arg 0: StringInfo state containing accumulated string data (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (validates aggregate execution context)
  - cstring_to_text_with_len (converts C string to PostgreSQL text with specified length)
  - PG_ARGISNULL, PG_GETARG_POINTER (PostgreSQL argument macros)
  - PG_RETURN_TEXT_P, PG_RETURN_NULL (PostgreSQL return macros)

- Called from:
  - PostgreSQL aggregate execution framework (not directly referenced in source)

## Notes and Other Information
- Uses Assert instead of explicit error checking for aggregate context validation
- The cursor field usage is critical - it was set by string_agg_transfn to store first delimiter length
- This design allows proper handling of parallel aggregation where delimiters must be preserved during intermediate phases
- The function handles the edge case where no non-NULL values were processed (returns NULL)
- Efficient text creation using cstring_to_text_with_len avoids unnecessary string copying
- The final step in the string_agg aggregate execution pipeline
- Works for both regular and parallel execution scenarios