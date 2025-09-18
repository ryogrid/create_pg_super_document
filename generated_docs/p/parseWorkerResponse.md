# parseWorkerResponse

## Location
[src/bin/pg_dump/parallel.c:1171-1204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1171-L1204)

## Overview
Parses status messages returned by worker processes in pg_dump parallel operations and extracts status information including dump ID, status code, and error count.

## Definition
```c
static int parseWorkerResponse(ArchiveHandle *AH, TocEntry *te, const char *msg)
```

## Detailed Description
This function is responsible for interpreting status messages sent back from worker processes during parallel pg_dump operations. It specifically handles "OK" status messages by parsing them to extract three key pieces of information: the dump ID, status code, and number of errors encountered. The function performs validation by asserting that the parsed dump ID matches the expected dump ID from the TocEntry, and that the entire message was consumed during parsing. Any errors encountered by the worker are accumulated in the main ArchiveHandle structure.

## Parameters / Member Variables
- `AH`: Pointer to the main ArchiveHandle structure that manages the dump operation state
- `te`: Pointer to the TocEntry representing the specific database object being processed
- `msg`: The status message string received from the worker process

## Dependencies
- Functions called/Symbols referenced:
  - messageStartsWith
  - [TocEntry](../T/TocEntry.md) (struct)
  - DumpId (type)
- Called from (representative examples):
  - [ListenToWorkers](../L/ListenToWorkers.md)

## Notes and Other Information
- The function only handles "OK" status messages; any other message format causes a fatal error
- Error counts from workers are accumulated in the main ArchiveHandle to provide overall error tracking
- The function uses assertions to validate message integrity and consistency
- This is part of the parallel dump infrastructure that coordinates multiple worker processes