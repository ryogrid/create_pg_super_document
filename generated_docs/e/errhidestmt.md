# errhidestmt

## Location
src/backend/utils/error/elog.c: 1411 - 1429

## Overview
Controls whether the STATEMENT field should be suppressed in log entries for the current error message.

## Definition
```c
int errhidestmt(bool hide_stmt)
```

## Detailed Description
This function optionally suppresses the STATEMENT: field from appearing in log entries. It should be called when the message text already includes the statement information, preventing redundant display of the same SQL statement in both the error message and the separate STATEMENT field. The function modifies the hide_stmt flag in the current error data structure on the error stack.

## Parameters / Member Variables
- `hide_stmt`: Boolean flag indicating whether to hide (true) or show (false) the STATEMENT field in log entries

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
- Called from (representative examples):
  - [llvm_compile_module](../l/llvm_compile_module.md) (src/backend/jit/llvm/llvmjit.c)
  - [exec_simple_query](exec_simple_query.md) (src/backend/tcop/postgres.c)
  - [exec_parse_message](exec_parse_message.md) (src/backend/tcop/postgres.c)
  - [exec_bind_message](exec_bind_message.md) (src/backend/tcop/postgres.c)
  - [exec_execute_message](exec_execute_message.md) (src/backend/tcop/postgres.c)
  - [MemoryContextStatsDetail](../M/MemoryContextStatsDetail.md) (src/backend/utils/mmgr/mcxt.c)
  - errcontext (macro in src/include/utils/elog.h)

## Notes and Other Information
- The function always returns 0, as the return value is not meaningful
- Does not increment recursion_depth as it's a simple flag setting operation
- Commonly used in query processing functions where the statement is already included in error messages
- Part of PostgreSQL's error reporting system that helps avoid duplicate information in logs
- Particularly useful for avoiding redundancy when custom error messages already contain the SQL statement text