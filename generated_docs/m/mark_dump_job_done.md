# mark_dump_job_done

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2541-2555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2541-L2555)

## Overview
A callback function invoked in the leader process after a step has been parallel dumped, primarily used to check for worker process failures and log completion status.

## Definition

```c
static void
mark_dump_job_done(ArchiveHandle *AH,
				   TocEntry *te,
				   int status,
				   void *callback_data)
```
## Detailed Description
This function serves as a completion callback for parallel dump operations in PostgreSQL's pg_dump utility. It is called by the leader process when a worker process finishes dumping a particular database object. The function's primary responsibilities are to log the completion of the dump item and to detect and handle worker process failures. If a worker process fails (indicated by a non-zero status), the function terminates the entire dump operation with a fatal error.

## Parameters / Member Variables
- : Archive handle containing the dump state and configuration (unused in this function)
- : Table of Contents entry representing the database object that was dumped
- : Exit status of the worker process (0 for success, non-zero for failure)
- : Additional callback data (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - [TocEntry](../T/TocEntry.md) (struct type)
- Called from (representative examples):
  - [WriteDataChunks](../W/WriteDataChunks.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_backup_archiver.c source file
- The function logs the completion using the dump ID, description, and tag from the TocEntry
- Any non-zero status from a worker process results in immediate termination of the entire dump operation
- The function follows the callback pattern typical in parallel processing frameworks
- The AH and callback_data parameters are not used in the current implementation but are part of the callback interface for potential future extensions