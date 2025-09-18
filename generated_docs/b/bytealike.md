# bytealike

## Location
[src/backend/utils/adt/like.c:324-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L324-L344)

## Overview
Implements the LIKE operator for PostgreSQL's bytea data type, returning true when binary data matches a given binary pattern.

## Definition


## Detailed Description
The `bytealike` function provides the implementation for PostgreSQL's LIKE operator when applied to bytea (binary data) data types. It takes two bytea arguments - the binary data to be tested and the binary pattern to match against - and uses single-byte pattern matching functionality to determine if the binary data matches the pattern. This function returns true when the pattern matches the binary data, and false when it does not match.

Unlike text-based LIKE operations, this function uses `SB_MatchText` (Single-Byte Match Text) instead of `GenericMatchText`, as binary data does not require character encoding considerations or collation support. The function treats the data as raw bytes without any character interpretation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `str` (bytea): The binary data to be tested against the pattern
  - `pat` (bytea): The binary pattern to match against, which may contain wildcards

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP: Extracts bytea arguments from function arguments with potential detoasting
  - VARDATA_ANY: Gets pointer to variable-length binary data for both data and pattern
  - VARSIZE_ANY_EXHDR: Gets size of variable-length binary data excluding header for both arguments
  - SB_MatchText: Single-byte pattern matching function specifically for binary data
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
  - LIKE_TRUE: Constant representing a successful pattern match
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator framework)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's operator system when the LIKE operator is used with bytea data types
- Uses `SB_MatchText` instead of `GenericMatchText` because binary data doesn't require character encoding or collation considerations
- The function treats the data as raw bytes, making it suitable for binary pattern matching
- Does not use collation (passes 0 as collation parameter to SB_MatchText) since binary data has no linguistic meaning
- The `true` parameter passed to SB_MatchText indicates case-sensitive matching (though this is less relevant for binary data)
- Handles variable-length binary data properly by extracting both data pointers and sizes