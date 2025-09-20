# buildWorkerResponse

## Location
[src/bin/pg_dump/parallel.c:1156-1170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1156-L1170)

## Overview
Formats response strings that worker processes send back to the leader after completing assigned dump or restore operations.

## Definition

```c
static void
buildWorkerResponse(ArchiveHandle *AH, TocEntry *te, T_Action act, int status,
					char *buf, int buflen)
```
## Detailed Description
buildWorkerResponse constructs standardized response messages that worker processes send to the leader process through inter-process communication channels after completing their assigned tasks. The function creates a response string containing the dump ID, completion status, and error count information. The response format is consistent across all archive formats and provides essential feedback for the leader to track worker progress and handle any errors that occurred during processing. The error count is only included when the worker encountered errors but chose to ignore them.

## Parameters / Member Variables
- : Archive handle containing error count information
- : Table of contents entry that was processed, providing the dump ID
- : Action type that was performed (not currently used in response formatting)
- : Completion status code indicating success, failure, or ignored errors
- : Caller-supplied buffer to store the formatted response string
- : Size of the buffer to prevent buffer overflows

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (formatted string construction)
  - WORKER_IGNORED_ERRORS (status constant for conditional error reporting)
- Called from (representative examples):
  - [WaitForCommands](../W/WaitForCommands.md) (src/bin/pg_dump/parallel.c:1372)

## Notes and Other Information
- Response format is "OK <dumpId> <status> <errorCount>"
- Error count is included only when status equals WORKER_IGNORED_ERRORS, otherwise it's 0
- Uses snprintf for safe string formatting with buffer bounds checking
- Static function scope limits visibility to the parallel.c module
- The act parameter is currently unused but maintained for potential future extensions
- Provides a standardized communication protocol between workers and leader for status reporting
- The response enables the leader to track completion of individual tasks and aggregate error information across all workers