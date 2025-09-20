# internalerrquery

## Location
[src/backend/utils/error/elog.c:1482-1511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1482-L1511)

## Overview
A function that adds internal query text to the current error context, or removes it if NULL is passed, primarily used in error callback subroutines for customizing error report layout.

## Definition

```c
int
internalerrquery(const char *query)
```
## Detailed Description
The  function manages the internal query text associated with the current error being constructed in PostgreSQL's error handling system. It operates on the current error data structure at the top of the error stack. The function can either set new internal query text by duplicating the provided string into the appropriate memory context, or clear existing internal query text by passing NULL. This is particularly useful in error callback subroutines that need to editorialize on the layout of error reports by adding or removing internal query information.

## Parameters / Member Variables
- : A string containing the internal query text to be associated with the current error, or NULL to remove any existing internal query text

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (structure type)
  - CHECK_STACK_DEPTH (macro for stack validation)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (memory allocation function)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from (representative examples):
  - [function_parse_error_transpose](../f/function_parse_error_transpose.md) (in pg_proc.c)
  - [import_error_callback](import_error_callback.md) (in foreigncmds.c)
  - [sql_exec_error_callback](../s/sql_exec_error_callback.md) (in functions.c)
  - [_SPI_error_callback](../S/_SPI_error_callback.md) (in spi.c)
  - [sql_inline_error_callback](../s/sql_inline_error_callback.md) (in clauses.c)
  - [PLy_elog_impl](../P/PLy_elog_impl.md) (in plpy_elog.c)

## Notes and Other Information
- The function always returns 0, indicating the return value is not significant
- Memory management is handled automatically - existing internal query text is freed before setting new text
- The function does not increment recursion_depth, unlike some other error functions
- Located in src/backend/utils/error/elog.c:1482-1511
- Part of PostgreSQL's comprehensive error handling system for providing detailed error context