# _SPI_end_call

## Location
src/backend/executor/spi.c: 3101 - 3116

## Overview
_SPI_end_call is a static internal function that concludes a SPI operation within a connected procedure, handling memory context cleanup and subtransaction state reset.

## Definition

```c
static int
_SPI_end_call(bool use_exec)
```
## Detailed Description
This function completes a SPI (Server Programming Interface) operation that was initiated by _SPI_begin_call. It performs the cleanup and restoration activities necessary to properly conclude the SPI operation and return the system to its previous state.

When use_exec is true (matching the parameter used in the corresponding _SPI_begin_call), the function performs three critical cleanup operations: switches back to the procedure memory context, marks the Executor context as no longer in use by setting the subtransaction ID to invalid, and resets the Executor memory context to free any allocated memory.

The function is designed to always succeed and currently has no failure cases, which is why callers typically don't check its return value.

## Parameters / Member Variables
- : Boolean flag that must match the value used in the corresponding _SPI_begin_call. When true, triggers memory context restoration and cleanup operations.

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_procmem: Switches back to the procedure memory context
  - InvalidSubTransactionId: Constant used to mark the Executor context as no longer in use
  - MemoryContextReset: Frees all memory allocated in the Executor context
- Called from (representative examples):
  - SPI_execute: Main SPI execution function
  - SPI_execute_plan: Executes prepared SPI plans
  - SPI_prepare_extended: Prepares extended SPI statements
  - SPI_cursor_open_internal: Opens SPI cursors
  - _SPI_cursor_operation: Performs cursor operations

## Notes and Other Information
- This is a static function internal to the SPI implementation, not part of the public SPI API
- Must be called with the same use_exec parameter value as the corresponding _SPI_begin_call
- Currently designed to never fail, so return value checking is not required by callers
- Essential for proper memory management and preventing memory leaks in SPI operations
- The memory context reset ensures that temporary allocations made during SPI execution are properly cleaned up
- Always paired with _SPI_begin_call to provide proper bracketing of SPI operations