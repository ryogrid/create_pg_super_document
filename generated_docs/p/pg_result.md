# pg_result

## Location
src/interfaces/libpq/libpq-int.h: 170 - 220

## Overview
The core internal structure in libpq that represents a query result set, containing data, metadata, and management information for PostgreSQL query results.

## Definition


## Detailed Description
The `pg_result` structure is the fundamental data structure that represents the result of a PostgreSQL query in libpq. It contains the actual data returned by the server, metadata about columns and parameters, error information, and memory management data. This structure is used internally and exposed to applications through the `PGresult` typedef.

The structure efficiently manages both successful query results and error conditions, providing a unified interface for accessing result data, column metadata, and error details. It also includes an event system for extensibility and memory management facilities for efficient allocation and cleanup.

## Parameters / Member Variables
- `ntups`: Number of tuples (rows) in the result set
- `numAttributes`: Number of attributes (columns) in each tuple
- `attDescs`: Array of column descriptors containing metadata about each attribute
- `tuples`: Two-dimensional array containing the actual data values
- `tupArrSize`: Allocated size of the tuples array (may be larger than ntups)
- `numParameters`: Number of parameters in prepared statements
- `paramDescs`: Array of parameter descriptors for prepared statements
- `resultStatus`: Execution status indicating success, error type, or command completion
- `cmdStatus`: String containing command status information from the server
- `binary`: Flag indicating whether tuple values are in binary (1) or text (0) format
- `noticeHooks`: Notice handling callbacks copied from the originating connection
- `events`: Array of registered event handlers
- `nEvents`: Number of registered event handlers
- `client_encoding`: Character encoding identifier for text data
- `errMsg`: Overall error message string, NULL if no error occurred
- `errFields`: Linked list of structured error message fields
- `errQuery`: Text of the query that caused an error, if available
- `null_field`: Single-character array used as target for all NULL attribute values
- `curBlock`: Pointer to the most recently allocated memory block
- `curOffset`: Offset to the start of free space in the current block
- `spaceLeft`: Number of free bytes remaining in the current memory block
- `memorySize`: Total memory allocated for this result structure

## Dependencies
- Functions called/Symbols referenced:
  - PGresAttDesc, PGresAttValue, PGresParamDesc (data structures)
  - ExecStatusType (execution status enumeration)
  - PGNoticeHooks, PGEvent, PGMessageField (event and error handling)
  - PGresult_data (memory management)
- Called from (representative examples):
  - Aliased as `PGresult` in public API via typedef
  - Used throughout libpq for query result management

## Notes and Other Information
- This is the internal representation; applications use the `PGresult` typedef
- Implements efficient memory management with block allocation to reduce fragmentation
- Supports both text and binary result formats from PostgreSQL protocol
- Contains complete error information including structured message fields
- Event system allows applications to extend functionality with custom handlers
- The `null_field` optimization allows all NULL values to point to the same memory location
- Memory is managed in blocks through `PGresult_data` to minimize allocation overhead
- Designed to be self-contained so operations don't require access to the originating connection