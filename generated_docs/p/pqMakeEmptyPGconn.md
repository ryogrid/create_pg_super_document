# pqMakeEmptyPGconn

## Location
[src/interfaces/libpq/fe-connect.c:4535-4633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L4535-L4633)

## Overview
Creates and initializes an empty PGconn data structure with default values, serving as the foundation for PostgreSQL client connections.

## Definition

```c
structure
 *
 * NOTE: this should not overlap any functionality with pqClosePGconn().
 * Clearing/resetting of transient state belongs there;
```
## Detailed Description
The  function is responsible for allocating and initializing a new  structure with sensible default values. This function creates the fundamental connection object that will later be configured and used to establish connections to PostgreSQL servers.

The function performs several critical initialization tasks:
- Allocates memory for the main PGconn structure
- Initializes all fields to safe default values
- Sets up input/output buffers with optimal sizes (16KB each)
- Configures notice handling hooks
- Initializes error message and work buffers
- On Windows, ensures WSA (Windows Sockets API) is properly initialized

The buffer sizing strategy is designed for performance: 16KB buffers with 8KB threshold for flushing to minimize kernel context switches during data transfer operations.

## Parameters / Member Variables
This function takes no parameters and returns a pointer to the newly created PGconn structure.

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (memory initialization)
  -  (default notice callback)
  -  (default notice processor)
  -  (buffer initialization)
  -  (buffer validation)
  -  (cleanup on failure)
  - Various constants: , , , , etc.

- Called from (representative examples):
  - 
  - 
  - 
  - 

## Notes and Other Information
- Returns NULL if memory allocation fails
- On Windows, performs one-time WSA initialization and does not call WSACleanup()
- Sets initial buffer sizes to 16KB for both input and output buffers
- Initializes row buffer to handle 32 PGdataValue entries initially
- Sets connection status to  until actual connection is established
- All boolean and pointer fields are zero-initialized via MemSet
- Client encoding defaults to SQL_ASCII
- Error verbosity defaults to 
- Socket is initialized to 
- Pipeline status defaults to 
- The function is designed to fail gracefully, calling  if any allocation fails