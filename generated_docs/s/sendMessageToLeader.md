# sendMessageToLeader

## Location
[src/bin/pg_dump/parallel.c:1527-1539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1527-L1539)

## Overview
Sends a status message from a worker process to the leader process through a communication pipe in the PostgreSQL parallel dump utility.

## Definition


## Detailed Description
This function is executed within worker processes to communicate status messages back to the leader process during parallel database dump operations. It writes the provided string message to the write end of a pipe, ensuring proper error handling if the write operation fails. The function calculates the message length including the null terminator and performs a complete write operation through the  utility function.

## Parameters / Member Variables
- : Array containing file descriptors for the communication pipe, where  is used for writing
- : Null-terminated string message to be sent to the leader process

## Dependencies
- Functions called/Symbols referenced:
  - : Low-level pipe write function for reliable message transmission
  - : Constant defining the write end index of the pipe array
  - : Standard C library function to calculate string length
  - : PostgreSQL error reporting function for fatal errors

- Called from (representative examples):
  - : Error message reporting function
  - : Worker process command waiting loop

## Notes and Other Information
- This function is marked as , indicating it's only used within the parallel.c module
- The function includes the null terminator in the message length calculation (len = strlen(str) + 1)
- Fatal error handling ensures that communication failures are properly reported and terminate the process
- Part of the PostgreSQL pg_dump parallel processing infrastructure for coordinating multiple worker processes