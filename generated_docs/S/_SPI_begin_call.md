# _SPI_begin_call

## Location
[src/backend/executor/spi.c:3077-3100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3077-L3100)

## Overview
_SPI_begin_call is a static internal function that initiates a SPI operation within a connected procedure, managing memory context and subtransaction tracking.

## Definition

```c
static int
_SPI_begin_call(bool use_exec)
```
## Detailed Description
This function begins a SPI (Server Programming Interface) operation within a procedure that has already established a SPI connection. It performs two key responsibilities: validating that a SPI connection exists and optionally setting up the execution memory context for operations that will use the procedure's execution context.

When use_exec is true, the function records the current subtransaction ID for proper cleanup and switches to the Executor memory context. This ensures that memory allocations during the SPI operation are properly managed and can be cleaned up when the operation completes or if an error occurs.

The function is designed to work in conjunction with _SPI_end_call to provide proper bracketing of SPI operations, ensuring proper resource management and error handling.

## Parameters / Member Variables
- : Boolean flag indicating whether the procedure's execution context will be used during this SPI operation. When true, triggers memory context switching and subtransaction tracking.

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md): Records the current subtransaction ID for cleanup purposes
  - [_SPI_execmem](_SPI_execmem.md): Switches to the Executor memory context
  - SPI_ERROR_UNCONNECTED: Error code returned when no SPI connection exists
- Called from (representative examples):
  - [SPI_execute](SPI_execute.md): Main SPI execution function
  - [SPI_execute_plan](SPI_execute_plan.md): Executes prepared SPI plans
  - [SPI_prepare_extended](SPI_prepare_extended.md): Prepares extended SPI statements
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md): Opens SPI cursors
  - [_SPI_cursor_operation](_SPI_cursor_operation.md): Performs cursor operations

## Notes and Other Information
- This is a static function internal to the SPI implementation, not part of the public SPI API
- Must be called only when a SPI connection has been established via SPI_connect
- Always paired with _SPI_end_call to ensure proper cleanup
- The subtransaction ID tracking enables proper cleanup if the operation is aborted
- Memory context switching ensures SPI operations use appropriate memory management

## Simplified Source

```c
static int
_SPI_begin_call(bool use_exec)
{
    // Check that SPI connection exists
    if (_SPI_current == NULL)
        return SPI_ERROR_UNCONNECTED;

    // If using execution context, set up tracking and switch contexts
    if (use_exec)
    {
        // Record subtransaction ID for cleanup
        _SPI_current->execSubid = GetCurrentSubTransactionId();

        // Switch to executor memory context
        _SPI_execmem();
    }

    return 0;
}
```