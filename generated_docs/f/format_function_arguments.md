# format_function_arguments

## Location
[src/bin/pg_dump/pg_dump.c:12260-12282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12260-L12282)

## Overview
The format_function_arguments function generates a properly formatted function name and argument list string for use in SQL dump output.

## Definition

```c
static char *
format_function_arguments(const FuncInfo *finfo, const char *funcargs, bool is_agg)
```
## Detailed Description
This utility function creates a formatted string containing a function name followed by its argument list in parentheses. It relies on pg_get_function_arguments to provide the argument formatting, but handles a special case for zero-argument aggregates where pg_get_function_arguments does not provide the expected format.

For aggregate functions with zero arguments, the function explicitly adds "(*)" to indicate the aggregate operates on all rows. For all other cases, it uses the provided funcargs string within parentheses.

The function returns a dynamically allocated string that the caller is responsible for freeing.

## Parameters / Member Variables
- : FuncInfo structure containing function metadata, particularly the function name
- : Pre-formatted argument list string from pg_get_function_arguments
- : Boolean flag indicating whether this is an aggregate function

## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](fmtId.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [PQExpBufferData](../P/PQExpBufferData.md)
- Called from (representative examples):
  - [dumpFunc](../d/dumpFunc.md)
  - [dumpAgg](../d/dumpAgg.md)

## Notes and Other Information
- Returns dynamically allocated memory that must be freed by the caller
- Special handling for zero-argument aggregates adds "(*)" instead of "()"
- Uses PQExpBuffer for efficient string building
- The function name is properly quoted using fmtId for SQL safety
- Designed to work with pg_get_function_arguments output for consistent formatting
- Used primarily in function and aggregate dumping contexts