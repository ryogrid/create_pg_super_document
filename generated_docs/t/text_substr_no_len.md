# text_substr_no_len

## Location
[src/backend/utils/adt/varlena.c:866-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L866-L884)

## Overview
A PostgreSQL function wrapper that extracts a substring from text starting at a specified position, continuing to the end of the string.

## Definition

```c
Datum
text_substr_no_len(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a wrapper around the more general  function to provide a simplified interface for extracting substrings without specifying an explicit length. It internally calls  with a length parameter of -1, which indicates that the substring should extend to the end of the input string. The function exists primarily to avoid opr_sanity failures that would occur if the same function accepted different numbers of arguments.

The function follows PostgreSQL's function call convention, taking arguments through the  mechanism and returning a  containing the result text.

## Parameters / Member Variables
- Uses : The input text from which to extract the substring
- Uses : The starting position (1-based indexing)
- Hardcoded length of -1: Indicates substring should continue to end of string
- Hardcoded  for : Indicates no explicit length was provided

## Dependencies
- Functions called/Symbols referenced:
  - [text_substring](text_substring.md)
  - PG_RETURN_TEXT_P
  - PG_GETARG_DATUM
  - PG_GETARG_INT32
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a thin wrapper function designed to maintain API compatibility
- The function automatically extends the substring to the end of the input text
- Uses PostgreSQL's standard function calling conventions with PG_FUNCTION_ARGS
- Part of PostgreSQL's variable-length character data handling utilities
- Located in src/backend/utils/adt/varlena.c, which contains various variable-length data type operations

## Simplified Source

```c
Datum text_substr_no_len(PG_FUNCTION_ARGS) {
    // Extract substring from start position to end of string
    PG_RETURN_TEXT_P(text_substring(PG_GETARG_DATUM(0),    // text input
                                   PG_GETARG_INT32(1),     // start position
                                   -1,                     // length = -1 (to end)
                                   true));                 // no explicit length
}
```