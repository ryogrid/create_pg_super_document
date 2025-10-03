# disconnect_atexit

## Location
[src/bin/pg_basebackup/pg_recvlogical.c:178-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_recvlogical.c#L178-L184)

## Overview
The disconnect_atexit function is an atexit handler that ensures proper cleanup of PostgreSQL database connections when the program terminates.

## Definition

```c
static void
disconnect_atexit(void)
```
## Detailed Description
This is a simple cleanup function designed to be registered with the atexit() system call. It ensures that any active PostgreSQL connection is properly closed when the program exits, whether through normal termination or due to an error condition. The function checks if a global connection handle exists and calls PQfinish() to cleanly close the connection and free associated resources.

This pattern is commonly used in PostgreSQL client utilities to prevent connection leaks and ensure proper cleanup even when the program exits unexpectedly. The function operates on a global  variable that represents the active PostgreSQL connection.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [PQfinish](../P/PQfinish.md) (libpq function to close PostgreSQL connection)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_basebackup.c:2814)
  - [main](../m/main.md) (in pg_receivewal.c:835)  
  - [main](../m/main.md) (in pg_recvlogical.c:936)
  - [main](../m/main.md) (in pg_rewind.c:300)
  - [main](../m/main.md) (in isolationtester.c:151)

## Notes and Other Information
- Static function registered as an atexit handler in various PostgreSQL utilities
- Provides cleanup safety net for unexpected program termination
- Operates on a global connection variable that must be in scope
- Part of defensive programming practices to prevent resource leaks
- Used consistently across multiple PostgreSQL client utilities (pg_basebackup, pg_receivewal, pg_recvlogical, pg_rewind, etc.)
- Simple but important for maintaining connection hygiene in client applications