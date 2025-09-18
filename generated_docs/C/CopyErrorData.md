# CopyErrorData

## Location
src/backend/utils/error/elog.c: 1746 - 1817

## Overview
Creates a deep copy of the topmost error stack entry for use in error handler code, ensuring the error data persists beyond the ErrorContext memory cleanup.

## Definition


## Detailed Description
CopyErrorData creates a complete copy of the current error stack entry (topmost error) into the current memory context. This function is specifically designed for use in error handler code where the error information needs to survive beyond the automatic cleanup performed by FlushErrorState. The function performs a deep copy of the ErrorData structure, including all separately-allocated string fields like filename, funcname, message, detail, hint, context, and various database object names. All string fields are copied using pstrdup() to ensure they remain valid even if the original strings point to JIT-created code segments that might be unloaded during transaction cleanup.

## Parameters / Member Variables
This function takes no parameters and returns a pointer to the copied ErrorData structure.

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (structure type)
  - CHECK_STACK_DEPTH (macro)
  - palloc (memory allocation)
  - memcpy (memory copy)
  - pstrdup (string duplication)
  - CurrentMemoryContext (global variable)
  - ErrorContext (global variable)
  - errordata (global array)
  - errordata_stack_depth (global variable)

- Called from (representative examples):
  - _SPI_commit
  - _SPI_rollback  
  - plperl_spi_exec
  - PLy_output
  - pltcl_elog

## Notes and Other Information
- Must be called with CurrentMemoryContext != ErrorContext to prevent data loss when FlushErrorState clears ErrorContext
- Does not increment recursion_depth as out-of-memory conditions here don't indicate error subsystem problems
- Copies even theoretically-constant strings like filename to handle JIT code segment unloading scenarios
- The copied ErrorData can survive transaction boundaries, making it suitable for deferred error handling
- All string fields are null-checked before copying to avoid segmentation faults