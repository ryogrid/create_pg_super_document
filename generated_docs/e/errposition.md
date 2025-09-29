# errposition

## Location
[src/backend/utils/error/elog.c:1446-1461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1446-L1461)

## Overview
Adds cursor position information to the current error message for precise error location reporting in SQL statements.

## Definition
```c
int errposition(int cursorpos)
```

## Detailed Description
This function sets the cursor position in the current error data structure, indicating the exact character position within a SQL statement where an error occurred. This position information is used to provide precise error location reporting to clients, helping users identify exactly where in their SQL statement the problem lies. The cursor position is typically a 1-based character offset from the beginning of the query string.

## Parameters / Member Variables
- `cursorpos`: Integer representing the character position within the SQL statement where the error occurred (typically 1-based)

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (struct type)
  - CHECK_STACK_DEPTH (macro)
- Called from (representative examples):
  - [function_parse_error_transpose](../f/function_parse_error_transpose.md) (src/backend/catalog/pg_proc.c)
  - [import_error_callback](../i/import_error_callback.md) (src/backend/commands/foreigncmds.c)
  - [executor_errposition](executor_errposition.md) (src/backend/executor/execUtils.c)
  - [sql_exec_error_callback](../s/sql_exec_error_callback.md) (src/backend/executor/functions.c)
  - [_SPI_error_callback](../S/_SPI_error_callback.md) (src/backend/executor/spi.c)
  - [sql_inline_error_callback](../s/sql_inline_error_callback.md) (src/backend/optimizer/util/clauses.c)
  - [parser_errposition](../p/parser_errposition.md) (src/backend/parser/parse_node.c)
  - errcontext (macro in src/include/utils/elog.h)

## Notes and Other Information
- The function always returns 0, as the return value is not meaningful
- Does not increment recursion_depth as it's a simple position setting operation
- Cursor position is typically 1-based, following SQL standard conventions
- Widely used throughout the parser, executor, and SPI systems for precise error reporting
- Essential for providing user-friendly error messages that point to the exact location of syntax or semantic errors
- The position information is used by client applications to highlight the problematic part of SQL statements

## Simplified Source

```c
int errposition(int cursorpos) {
    // Get current error data structure
    ErrorData *edata = &errordata[errordata_stack_depth];

    // Check stack depth is valid
    CHECK_STACK_DEPTH();

    // Set the cursor position for error reporting
    edata->cursorpos = cursorpos;

    return 0;  // Return value not used
}
```