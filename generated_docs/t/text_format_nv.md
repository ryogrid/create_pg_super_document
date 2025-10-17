# text_format_nv

## Location
[src/backend/utils/adt/varlena.c:6142-6151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6142-L6151)

## Overview
A non-variadic wrapper function for text_format that ensures compatibility with PostgreSQL's built-in function argument sanity checks.

## Definition

```c
Datum
text_format_nv(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a simple wrapper around the text_format function. Its primary purpose is to satisfy PostgreSQL's opr_sanity system checks, which verify that all built-in functions sharing the same implementing C function take the same number of arguments. The wrapper provides a non-variadic interface to the variadic text_format function, allowing it to be used in contexts where a fixed number of arguments is expected.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [text_format](text_format.md)
- Called from:
  - No direct references found (likely referenced from PostgreSQL system catalogs)

## Notes and Other Information
- This wrapper exists solely for PostgreSQL's internal consistency requirements
- The function simply delegates all work to text_format by passing through the FunctionCallInfo
- Part of PostgreSQL's format() SQL function implementation architecture
- Required to maintain proper separation between variadic and non-variadic function interfaces
- The wrapper pattern is common in PostgreSQL for functions that need both variadic and fixed-argument variants

## Simplified Source

```c
Datum text_format_nv(PG_FUNCTION_ARGS) {
    // Simple non-variadic wrapper for text_format
    // Required for PostgreSQL's opr_sanity consistency checks
    return text_format(fcinfo);
}
```