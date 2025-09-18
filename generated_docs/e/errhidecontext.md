# errhidecontext

## Location
[src/backend/utils/error/elog.c:1430-1445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1430-L1445)

## Overview
Controls whether the CONTEXT field should be suppressed in log entries for the current error message.

## Definition
```c
int errhidecontext(bool hide_ctx)
```

## Detailed Description
This function optionally suppresses the CONTEXT: field from appearing in log entries. It is primarily intended for verbose debugging messages where the repeated inclusion of context information would unnecessarily bloat the log volume. The function modifies the hide_ctx flag in the current error data structure on the error stack, controlling whether context information will be displayed in the final log output.

## Parameters / Member Variables
- `hide_ctx`: Boolean flag indicating whether to hide (true) or show (false) the CONTEXT field in log entries

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
- Called from (representative examples):
  - [llvm_compile_module](../l/llvm_compile_module.md) (src/backend/jit/llvm/llvmjit.c)
  - [MemoryContextStatsDetail](../M/MemoryContextStatsDetail.md) (src/backend/utils/mmgr/mcxt.c)
  - [MemoryContextStatsInternal](../M/MemoryContextStatsInternal.md) (src/backend/utils/mmgr/mcxt.c)
  - [MemoryContextStatsPrint](../M/MemoryContextStatsPrint.md) (src/backend/utils/mmgr/mcxt.c)
  - [ProcessLogMemoryContextInterrupt](../P/ProcessLogMemoryContextInterrupt.md) (src/backend/utils/mmgr/mcxt.c)
  - errcontext (macro in src/include/utils/elog.h)

## Notes and Other Information
- The function always returns 0, as the return value is not meaningful
- Does not increment recursion_depth as it's a simple flag setting operation
- Should only be used for verbose debugging messages to prevent log bloat
- Commonly used in memory context debugging and JIT compilation contexts
- Part of PostgreSQL's error reporting system for controlling log verbosity
- Helps maintain readable logs by suppressing repetitive context information in debugging scenarios