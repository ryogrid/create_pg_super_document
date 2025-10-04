# parser_errposition

## Location
[src/backend/parser/parse_node.c:106-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_node.c#L106-L139)

## Overview
Reports a parse-analysis-time cursor position for error reporting by converting byte offsets to character positions.

## Definition

```c
int
parser_errposition(ParseState *pstate, int location)
```
## Detailed Description
The  function converts raw byte offsets from the parser into 1-based character indexes suitable for error reporting to clients. This function is designed to be used within  calls to provide precise error location information.

Raw parse trees store locations as byte offsets into the source string for efficiency. However, error reporting requires character positions that account for multibyte characters. The function performs this conversion using  to properly handle multibyte character encodings, then passes the result to the error reporting mechanism via .

The function includes safety checks to handle cases where location information is unavailable or the source text is not accessible, returning 0 (no-op) in such cases.

## Parameters / Member Variables
- `*pstate`: ParseState containing the source text for position calculation. Can be NULL, in which case the function returns 0.
- `location`: Byte offset into the source string. If negative, the function returns 0 without processing.
## Dependencies
- Functions called/Symbols referenced:
  -  (multibyte string length calculation)
  -  (error position reporting mechanism)
- Called from (representative examples):
  - Currently no direct references found in the analyzed codebase

## Notes and Other Information
- Always returns 0 as a dummy value since the actual work is done by 
- Handles multibyte character encodings correctly by using 
- Converts 0-based byte offsets to 1-based character positions for user-friendly error reporting
- Provides graceful fallback when location or source text information is unavailable
- Designed specifically for use within  error reporting calls
- Location: src/backend/parser/parse_node.c:106-139

## Simplified Source

```c
int
parser_errposition(ParseState *pstate, int location)
{
    int pos;

    // Return no-op if location not provided
    if (location < 0)
        return 0;

    // Return no-op if source text unavailable
    if (pstate == NULL || pstate->p_sourcetext == NULL)
        return 0;

    // Convert byte offset to character position (1-based)
    pos = pg_mbstrlen_with_len(pstate->p_sourcetext, location) + 1;

    // Pass to error reporting mechanism
    return errposition(pos);
}
```