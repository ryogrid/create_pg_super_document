# byteanlike

## Location
src/backend/utils/adt/like.c: 345 - 369

## Overview
Implements the NOT LIKE operator for PostgreSQL's bytea data type, returning true when binary data does not match a given binary pattern.

## Definition


## Detailed Description
The `byteanlike` function provides the implementation for PostgreSQL's NOT LIKE operator when applied to bytea (binary data) data types. It takes two bytea arguments - the binary data to be tested and the binary pattern to match against - and uses single-byte pattern matching functionality to determine if the binary data does NOT match the pattern. This is the inverse of the LIKE operation for binary data - it returns true when the pattern does not match the binary data, and false when it does match.

Like `bytealike`, this function uses `SB_MatchText` (Single-Byte Match Text) instead of `GenericMatchText`, as binary data does not require character encoding considerations or collation support. The function treats the data as raw bytes without any character interpretation, and then negates the result to implement NOT LIKE semantics.

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
- This function is typically invoked through PostgreSQL's operator system when the NOT LIKE operator is used with bytea data types
- Uses `SB_MatchText` instead of `GenericMatchText` because binary data doesn't require character encoding or collation considerations
- The result is negated compared to regular LIKE operations (returns true when pattern does NOT match)
- The function treats the data as raw bytes, making it suitable for binary pattern matching
- Does not use collation (passes 0 as collation parameter to SB_MatchText) since binary data has no linguistic meaning
- The `true` parameter passed to SB_MatchText indicates case-sensitive matching (though this is less relevant for binary data)
- This is the binary equivalent of `textnlike`, but operates on binary data instead of text
- Handles variable-length binary data properly by extracting both data pointers and sizes