# varchar

## Location
[src/backend/utils/adt/varchar.c:609-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L609-L647)

## Overview
Converts a VARCHAR value to a specified size with configurable truncation behavior, serving as the length coercion function for the VARCHAR data type.

## Definition

```c
Datum
varchar(PG_FUNCTION_ARGS)
```
## Detailed Description
The `varchar` function performs length coercion for VARCHAR values, converting them to fit within a specified maximum length constraint. This function implements the core logic for VARCHAR length enforcement in PostgreSQL, handling both explicit and implicit casts with different truncation rules.

For explicit casts (e.g., `column::varchar(10)`), the function silently truncates strings that exceed the specified length. For implicit casts, it raises an error unless the excess characters are all spaces, which can be safely removed. The function preserves multibyte character boundaries during truncation using `pg_mbcharcliplen` to avoid splitting multibyte characters.

The function optimizes for the common case where no truncation is needed by returning the original value unchanged when it already fits within the specified length constraint.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:
  - Argument 0: `VarChar *source` - The source VARCHAR value to be length-coerced
  - Argument 1: `int32 typmod` - Type modifier specifying the target length plus header size
  - Argument 2: `bool isExplicit` - Flag indicating whether this is an explicit cast

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_VARCHAR_PP`: Extracts VARCHAR argument, potentially detoasted
  - `PG_GETARG_INT32`: Extracts int32 argument (typmod)
  - `PG_GETARG_BOOL`: Extracts boolean argument (isExplicit)
  - `VARSIZE_ANY_EXHDR`: Gets size excluding header from variable-length data
  - `VARDATA_ANY`: Gets pointer to variable-length data
  - [pg_mbcharcliplen](../p/pg_mbcharcliplen.md): Clips string preserving multibyte boundaries
  - `ereport`: Reports errors with proper error codes
  - `cstring_to_text_with_len`: Converts C string to text with specified length
  - `PG_RETURN_VARCHAR_P`: Returns VARCHAR result
  - `VARHDRSZ`: Variable header size constant

- Called from (representative examples):
  - Type coercion operations in expressions
  - Column constraint enforcement
  - Explicit and implicit cast operations

## Notes and Other Information
- Implements SQL standard behavior for VARCHAR length constraints with PostgreSQL-specific extensions
- Handles multibyte character encodings correctly by preserving character boundaries
- Different behavior for explicit vs implicit casts follows SQL standards
- Optimized to avoid unnecessary copying when no truncation is needed
- Error messages include the specific length constraint that was violated
- The typmod parameter includes the VARHDRSZ offset, which must be subtracted to get the actual length limit
- Preserves the original data structure when possible for performance reasons