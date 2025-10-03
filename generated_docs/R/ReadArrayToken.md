# ReadArrayToken

## Location
[src/backend/utils/adt/arrayfuncs.c:796-960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L796-L960)

## Overview
Tokenizes array string input by reading one token at a time, handling quoted/unquoted elements, delimiters, braces, and escape sequences during array parsing.

## Definition

```c
static ArrayToken
ReadArrayToken(char **srcptr, StringInfo elembuf, char typdelim,
			   const char *origStr, Node *escontext)
```
## Detailed Description
ReadArrayToken is the fundamental lexical analyzer for PostgreSQL array input parsing. It implements a sophisticated tokenizer that can distinguish between different types of array tokens and handle complex quoting and escaping rules.

The function returns one of several ArrayToken types:
- **ATOK_LEVEL_START** ('{') - Start of a nested array level
- **ATOK_LEVEL_END** ('}') - End of a nested array level  
- **ATOK_DELIM** - Element delimiter (type-specific character)
- **ATOK_ELEM** - Regular element value
- **ATOK_ELEM_NULL** - NULL element (unquoted "NULL" string)
- **ATOK_ERROR** - Parse error occurred

The tokenizer handles three distinct parsing contexts:
1. **Token identification**: Skips whitespace and identifies token type
2. **Quoted elements**: Processes elements enclosed in double quotes with escape handling
3. **Unquoted elements**: Processes bare elements with special NULL detection

Key features:
- Supports backslash escaping in both quoted and unquoted contexts
- Removes trailing whitespace from unquoted elements
- Validates quoting consistency (elements must be fully quoted or unquoted)
- Recognizes unquoted "NULL" as a special null value when Array_nulls is enabled
- Provides detailed error messages for malformed input

## Parameters / Member Variables
- `**srcptr`: Pointer to current position in input string, advanced past the token
- `elembuf`: StringInfo buffer to store de-escaped element content
- `typdelim`: Type-specific delimiter character for array elements
- `*origStr`: Original input string (used only for error messages)
- `*escontext`: Error context for soft error handling
## Dependencies
- Functions called/Symbols referenced:
  - [resetStringInfo](../r/resetStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [scanner_isspace](../s/scanner_isspace.md)
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - ereturn
  - Array_nulls (global variable)
  - ArrayToken enum values (ATOK_*)
- Called from (representative examples):
  - [ReadArrayStr](ReadArrayStr.md)

## Notes and Other Information
- Static function internal to arrayfuncs.c
- Advances srcptr to point past the consumed token
- Uses elembuf only for ATOK_ELEM and ATOK_ELEM_NULL tokens
- Implements PostgreSQL's array literal syntax rules exactly
- Handles both PostgreSQL-style backslash escaping and standard SQL double-quote escaping
- The function is stateless and can be called repeatedly to tokenize an entire array
- Validates that elements are consistently quoted throughout
- Special handling for "NULL" string depends on Array_nulls configuration setting