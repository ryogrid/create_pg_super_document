# errposition

## Location
src/backend/utils/error/elog.c: 1446 - 1461

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
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
- Called from (representative examples):
  - function_parse_error_transpose (src/backend/catalog/pg_proc.c)
  - import_error_callback (src/backend/commands/foreigncmds.c)
  - executor_errposition (src/backend/executor/execUtils.c)
  - sql_exec_error_callback (src/backend/executor/functions.c)
  - _SPI_error_callback (src/backend/executor/spi.c)
  - sql_inline_error_callback (src/backend/optimizer/util/clauses.c)
  - parser_errposition (src/backend/parser/parse_node.c)
  - errcontext (macro in src/include/utils/elog.h)

## Notes and Other Information
- The function always returns 0, as the return value is not meaningful
- Does not increment recursion_depth as it's a simple position setting operation
- Cursor position is typically 1-based, following SQL standard conventions
- Widely used throughout the parser, executor, and SPI systems for precise error reporting
- Essential for providing user-friendly error messages that point to the exact location of syntax or semantic errors
- The position information is used by client applications to highlight the problematic part of SQL statements