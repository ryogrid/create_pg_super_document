# internalerrposition

## Location
[src/backend/utils/error/elog.c:1462-1481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1462-L1481)

## Overview
Adds internal cursor position information to the current error message for precise error location reporting in internally generated SQL statements.

## Definition
```c
int internalerrposition(int cursorpos)
```

## Detailed Description
This function sets the internal cursor position in the current error data structure, indicating the exact character position within an internally generated SQL statement where an error occurred. This is distinct from errposition() in that it's used for SQL statements that are generated internally by PostgreSQL (such as rewritten queries, function bodies, or triggered statements) rather than user-provided queries. The internal position information helps distinguish between errors in the original user query versus errors in PostgreSQL's internal query transformations.

## Parameters / Member Variables
- `cursorpos`: Integer representing the character position within the internal SQL statement where the error occurred (typically 1-based)

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (struct type)
  - CHECK_STACK_DEPTH (macro)
- Called from (representative examples):
  - [function_parse_error_transpose](../f/function_parse_error_transpose.md) (src/backend/catalog/pg_proc.c)
  - [import_error_callback](import_error_callback.md) (src/backend/commands/foreigncmds.c)
  - [sql_exec_error_callback](../s/sql_exec_error_callback.md) (src/backend/executor/functions.c)
  - [_SPI_error_callback](../S/_SPI_error_callback.md) (src/backend/executor/spi.c)
  - [sql_inline_error_callback](../s/sql_inline_error_callback.md) (src/backend/optimizer/util/clauses.c)
  - [PLy_elog_impl](../P/PLy_elog_impl.md) (src/pl/plpython/plpy_elog.c)
  - errcontext (macro in src/include/utils/elog.h)

## Notes and Other Information
- The function always returns 0, as the return value is not meaningful
- Does not increment recursion_depth as it's a simple position setting operation
- Used specifically for internally generated queries, distinguishing from user-provided query errors
- Commonly used in function execution, SPI contexts, and procedural language implementations
- Part of PostgreSQL's comprehensive error reporting system that provides context for both user and internal queries
- Helps developers and users understand whether an error originated from their SQL or from PostgreSQL's internal processing
- Essential for debugging complex scenarios involving query rewriting, function execution, and stored procedures

## Simplified Source

```c
int internalerrposition(int cursorpos) {
    // Get current error data structure
    ErrorData *edata = &errordata[errordata_stack_depth];

    // Check stack depth is valid
    CHECK_STACK_DEPTH();

    // Set the internal cursor position for error reporting
    edata->internalpos = cursorpos;

    return 0;  // Return value not used
}
```