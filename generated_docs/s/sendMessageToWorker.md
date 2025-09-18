# sendMessageToWorker

## Location
src/bin/pg_dump/parallel.c: 1644 - 1661

## Overview
Sends a command message from the leader process to a specified worker process through the parallel dump communication infrastructure.

## Definition


## Detailed Description
This function is executed in the leader process to send command messages to individual worker processes during parallel database dump operations. It writes the provided command string to the write end of the specified worker's communication pipe, ensuring the complete message including the null terminator is transmitted. The function provides error handling that terminates the process if the write operation fails, maintaining the integrity of the leader-worker communication protocol.

## Parameters / Member Variables
- : Pointer to ParallelState structure containing worker management and communication information
- : Index of the target worker process in the parallel slot array
- : Null-terminated command string to be sent to the worker process

## Dependencies
- Functions called/Symbols referenced:
  - : Structure type for managing parallel worker state and communication pipes
  - : Low-level pipe write function for reliable message transmission
  - : Standard C library function to calculate string length
  - : PostgreSQL error reporting function for fatal errors

- Called from (representative examples):
  - : Error message reporting function
  - : Function that assigns dump tasks to worker processes

## Notes and Other Information
- This function is marked as , limiting its scope to the parallel.c module
- Includes the null terminator in the message length calculation (len = strlen(str) + 1)
- Uses the  file descriptor from the worker's parallel slot for communication
- Fatal error handling ensures that communication failures are properly reported and terminate the process
- Counterpart to , forming the bidirectional communication system
- Critical component in PostgreSQL's parallel dump task distribution mechanism
- The function assumes the worker index is valid and the worker's pipe is properly initialized