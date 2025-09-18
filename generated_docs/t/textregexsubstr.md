# textregexsubstr

## Location
[src/backend/utils/adt/regexp.c:583-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L583-L641)

## Overview
A PostgreSQL function that extracts a substring from text based on a regular expression pattern match, returning either the first parenthesized subexpression or the entire match.

## Definition
```c
Datum textregexsubstr(PG_FUNCTION_ARGS)
```

## Detailed Description
The `textregexsubstr` function implements PostgreSQL's regular expression substring extraction functionality. It compiles and executes a regular expression against a text string, then returns the matched portion as a new text value. The function prioritizes parenthesized subexpressions - if the pattern contains parentheses, it returns the content matched by the first subexpression; otherwise, it returns the entire matched portion. This function supports PostgreSQL's advanced regex features and handles cases where patterns match but subexpressions don't.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: The source text string to search within
- `PG_GETARG_TEXT_PP(1)`: The regular expression pattern to match
- `re`: Compiled regular expression object
- `pmatch[2]`: Array of match result structures for overall match and first subexpression
- `so`, `eo`: Start and end offsets of the matched substring

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP`: Extracts text arguments with potential detoasting
  - [RE_compile_and_cache](../R/RE_compile_and_cache.md): Compiles and caches the regular expression
  - [RE_execute](../R/RE_execute.md): Executes the compiled regex against the text
  - `VARDATA_ANY`: Gets pointer to the actual data portion of text
  - `VARSIZE_ANY_EXHDR`: Gets the size of text data excluding header
  - `PG_GET_COLLATION`: Gets collation information for the operation
  - `DirectFunctionCall3`: Calls another PostgreSQL function directly
  - [text_substr](text_substr.md): Extracts substring from text using start position and length
  - [PointerGetDatum](../P/PointerGetDatum.md), `Int32GetDatum`: Convert values to PostgreSQL Datum format
  - `REG_ADVANCED`: Flag for advanced regular expression features
  - `regex_t`, `regmatch_t`: Regular expression data types
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL function dispatch)

## Notes and Other Information
- Returns NULL if no match is found
- Prioritizes parenthesized subexpressions over the full match
- Handles edge cases where full pattern matches but subexpressions don't
- Uses 1-based indexing for substring extraction (PostgreSQL convention)
- Leverages PostgreSQL's text_substr function for the final substring extraction
- Part of PostgreSQL's comprehensive regular expression functionality
- Typically called through SQL substring() function with regex patterns