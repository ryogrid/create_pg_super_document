# pg_set_noblock

## Location
[src/port/noblock.c:25-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/noblock.c#L25-L48)

## Overview
Sets a socket to non-blocking mode, allowing I/O operations to return immediately without waiting for completion.

## Definition


## Detailed Description
The  function configures a socket to operate in non-blocking mode. In non-blocking mode, I/O operations on the socket will return immediately rather than waiting for the operation to complete. This is essential for implementing asynchronous I/O and preventing the process from blocking on socket operations.

The function uses platform-specific system calls:
- On Unix-like systems: Uses  with  and  flags to modify the  flag
- On Windows: Uses  with the  command

## Parameters / Member Variables
- : A PostgreSQL socket descriptor ( type) that will be configured for non-blocking operation

## Dependencies
- Functions called/Symbols referenced:
  -  (Unix/Linux systems)
  -  (Windows systems)
  -  (PostgreSQL socket type)
- Called from (representative examples):
  -  (src/backend/libpq/pqcomm.c:294)
  -  (src/backend/postmaster/postmaster.c:3653)
  - Connection handling in libpq (src/interfaces/libpq/fe-connect.c:3055)

## Notes and Other Information
- Returns  on success,  on failure
- The function handles cross-platform differences between Unix/Linux and Windows socket APIs
- On Unix systems, failure can occur if  cannot get or set the socket flags
- On Windows,  returns 0 on success (unlike  which returns -1 on failure)
- This is a critical function for PostgreSQL's asynchronous I/O operations and connection management
- Should be used in conjunction with proper error handling for socket operations