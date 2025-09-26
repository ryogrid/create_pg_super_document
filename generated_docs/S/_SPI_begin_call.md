# _SPI_begin_call

## Location
src/backend/executor/spi.c: 3077 - 3100

## Overview
_SPI_begin_call is a static internal function that initiates a SPI operation within a connected procedure, managing memory context and subtransaction tracking.

## Definition


## Detailed Description
This function begins a SPI (Server Programming Interface) operation within a procedure that has already established a SPI connection. It performs two key responsibilities: validating that a SPI connection exists and optionally setting up the execution memory context for operations that will use the procedure's execution context.

When use_exec is true, the function records the current subtransaction ID for proper cleanup and switches to the Executor memory context. This ensures that memory allocations during the SPI operation are properly managed and can be cleaned up when the operation completes or if an error occurs.

The function is designed to work in conjunction with _SPI_end_call to provide proper bracketing of SPI operations, ensuring proper resource management and error handling.

## Parameters / Member Variables
- : Boolean flag indicating whether the procedure's execution context will be used during this SPI operation. When true, triggers memory context switching and subtransaction tracking.

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentSubTransactionId: Records the current subtransaction ID for cleanup purposes
  - _SPI_execmem: Switches to the Executor memory context
  - SPI_ERROR_UNCONNECTED: Error code returned when no SPI connection exists
- Called from (representative examples):
  - SPI_execute: Main SPI execution function
  - SPI_execute_plan: Executes prepared SPI plans
  - SPI_prepare_extended: Prepares extended SPI statements
  - SPI_cursor_open_internal: Opens SPI cursors
  - _SPI_cursor_operation: Performs cursor operations

## Notes and Other Information
- This is a static function internal to the SPI implementation, not part of the public SPI API
- Must be called only when a SPI connection has been established via SPI_connect
- Always paired with _SPI_end_call to ensure proper cleanup
- The subtransaction ID tracking enables proper cleanup if the operation is aborted
- Memory context switching ensures SPI operations use appropriate memory management