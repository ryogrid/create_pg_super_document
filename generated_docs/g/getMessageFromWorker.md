# getMessageFromWorker

## Location
[src/bin/pg_dump/parallel.c:1579-1643](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1579-L1643)

## Overview
Checks for and retrieves messages from worker processes in the PostgreSQL parallel dump leader process, managing inter-process communication through pipes.

## Definition


## Detailed Description
This function is executed in the leader process to monitor and receive status messages from worker processes during parallel database dump operations. It constructs a file descriptor set containing all active worker pipes, then uses select() to determine which workers have messages ready. The function supports both blocking and non-blocking modes depending on the  parameter. When a message is available, it reads the complete message from the first ready worker pipe and returns it as a dynamically allocated string, along with the worker index.

## Parameters / Member Variables
- : Pointer to ParallelState structure containing worker management information
- : Boolean flag indicating whether to block waiting for messages (true) or return immediately if no messages are available (false)
- : Output parameter that receives the index of the worker that sent the message

## Dependencies
- Functions called/Symbols referenced:
  - : Structure type for managing parallel worker state
  - : Macro to check if a worker process is currently active
  - : Helper function for robust select() operations with interrupt handling
  - : System call for monitoring file descriptor readiness
  - : Function to read complete messages from worker pipes
  - , , : Standard file descriptor set manipulation macros
  - : PostgreSQL error reporting function for fatal errors

- Called from (representative examples):
  - : Error message reporting function
  - : Main worker communication loop in leader process

## Notes and Other Information
- This function is marked as , limiting its scope to the parallel.c module
- Returns a malloc'd string that the caller is responsible for freeing
- Uses non-blocking select with zero timeout when  is false
- Implements a blocking read strategy since worker messages are short and sent atomically
- EOF detection on worker pipes returns NULL, which may be indistinguishable from no-data-available
- Critical component of the leader-worker communication protocol in PostgreSQL's parallel dump infrastructure
- The function iterates through workers in order, so message processing has deterministic ordering when multiple workers have pending messages