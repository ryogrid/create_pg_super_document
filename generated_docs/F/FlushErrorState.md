# FlushErrorState

## Location
src/backend/utils/error/elog.c: 1867 - 1891

## Overview
Resets the error subsystem state after error recovery, clearing the error stack and freeing all ErrorContext memory.

## Definition
```c
void FlushErrorState(void)
```

## Detailed Description
FlushErrorState is the cleanup function that must be called after error handling is complete to properly reset the error subsystem. This function performs a complete reset by setting the error stack depth back to empty (-1), resetting the recursion depth counter to 0, and clearing all data from the ErrorContext memory context. The function handles cases where multiple errors may have been stacked (e.g., when an error interrupts the construction of another error message) by completely resetting the stack rather than trying to unwind it. After calling this function, the error handler is considered to be "out" of the error subsystem.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md) (memory context cleanup)
  - errordata_stack_depth (global variable)
  - recursion_depth (global variable)
  - ErrorContext (global memory context)

- Called from (representative examples):
  - _SPI_commit
  - [_SPI_rollback](../S/_SPI_rollback.md)
  - [PostgresMain](../P/PostgresMain.md)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [plperl_spi_exec](../p/plperl_spi_exec.md)
  - [PLy_output](../P/PLy_output.md)
  - pltcl_elog

## Notes and Other Information
- Must be called by error handlers when done processing errors, or immediately after CopyErrorData if further error-prone operations are planned
- Completely resets the error stack rather than attempting partial cleanup
- Assumes that control has escaped from any interrupted error message construction
- Critical for preventing memory leaks in ErrorContext
- Used extensively throughout PostgreSQL in both backend processes and procedural language implementations
- The reset to stack depth -1 indicates an empty stack (0-based indexing with -1 as empty state)