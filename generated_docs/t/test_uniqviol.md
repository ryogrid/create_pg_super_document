# test_uniqviol

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:1921-2088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1921-L2088)

## Overview
Tests PostgreSQL pipeline behavior with mixed successful and error-producing queries using non-blocking I/O and prepared statements, specifically testing unique constraint violation handling.

## Definition
```c
static void test_uniqviol(PGconn *conn)
```

## Detailed Description
This sophisticated test function validates pipeline behavior in non-blocking mode when mixing successful queries with intentional unique constraint violations. The test implements a complex I/O pattern that:

1. **Setup**: Creates a test table with a primary key, begins a transaction, and prepares an INSERT statement
2. **Non-blocking Pipeline**: Switches to non-blocking mode and enters pipeline mode
3. **Mixed Query Stream**: Sends a stream of INSERT queries using `select()` for I/O multiplexing:
   - Most queries insert unique values (successful)
   - One query intentionally violates the primary key constraint (error)
4. **Socket Management**: Carefully manages socket fullness and reading/writing phases to avoid deadlocks
5. **Error Injection**: Strategically injects a uniqueness violation after switching to read mode
6. **Result Processing**: Uses `process_result()` helper function to validate and handle both successful and error results

The test demonstrates:
- Non-blocking pipeline I/O handling
- Proper error recovery in pipeline mode
- Socket buffer management and flow control
- Mixed success/error result processing

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (`PGconn *`) configured for non-blocking pipeline operations

## Dependencies
- Functions called/Symbols referenced:
  - [PQsocket](../P/PQsocket.md) - Get connection socket descriptor
  - [PQsetnonblocking](../P/PQsetnonblocking.md) - Enable non-blocking mode
  - [PQexec](../P/PQexec.md) - Execute immediate SQL commands
  - [PQprepare](../P/PQprepare.md) - Prepare a statement
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md) - Enter pipeline mode
  - [PQisBusy](../P/PQisBusy.md) - Check if connection is busy
  - [PQgetResult](../P/PQgetResult.md) - Retrieve query results
  - [PQconsumeInput](../P/PQconsumeInput.md) - Read available input from server
  - [PQsendQueryPrepared](../P/PQsendQueryPrepared.md) - Execute prepared statements
  - [PQsendFlushRequest](../P/PQsendFlushRequest.md) - Send flush request
  - [PQflush](../P/PQflush.md) - Flush outgoing data
  - [process_result](../p/process_result.md) - Helper function to process individual results
  - select - System call for I/O multiplexing
  - FD_ZERO, FD_SET, FD_ISSET - File descriptor set macros
  - [PQresultStatus](../P/PQresultStatus.md) - Get result status
  - [PQerrorMessage](../P/PQerrorMessage.md) - Get error message
  - PGRES_COMMAND_OK - [Command](../C/Command.md) executed successfully
  - MAXINT8LEN - Maximum length of int8 string representation
  - EINTR - Interrupted system call error code
- Called from (representative examples):
  - [main](../m/main.md) - Main test driver function

## Notes and Other Information
- This is an advanced test for non-blocking pipeline I/O with error handling
- Creates and uses a test table `ppln_uniqviol` with a bigint primary key
- Uses prepared statements for efficient repeated execution
- Implements sophisticated flow control to prevent deadlocks in non-blocking mode
- Intentionally triggers a unique constraint violation to test error handling
- Uses `select()` system call for proper I/O multiplexing between reading and writing
- The test manages socket buffer fullness and switches between read/write phases accordingly
- Part of the libpq_pipeline test module located in `src/test/modules/libpq_pipeline/`
- Demonstrates real-world patterns for high-performance database applications
- The error is injected strategically after the connection has switched to read mode
- Validates that exactly one error occurs during the entire operation
- Shows proper handling of mixed successful and failed operations in a single pipeline
- Uses parameterized queries with bigint parameters for the INSERT operations
- Implements proper cleanup and error detection throughout the complex I/O loop