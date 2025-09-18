# parallel_restore

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4611-4633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4611-L4633)

## Overview
Restores a single TOC item in a parallel worker process or thread during parallel backup/restore operations.

## Definition
int parallel_restore(ArchiveHandle *AH, TocEntry *te)

## Detailed Description
This function executes in worker processes (on Unix-like systems) or threads (on Windows) to restore individual TOC entries as part of a parallel restore operation. Each worker can handle multiple work items sequentially, receiving new assignments from the leader process after completing each item.

The function performs the actual restoration work for a single TOC entry by calling restore_toc_entry with parallel processing enabled. It maintains error counting specific to the current TOC entry and ensures the database connection is available before proceeding.

## Parameters / Member Variables
- AH: Archive handle containing the database connection and restore context
- te: TOC entry to be restored by this worker

## Dependencies
- Functions called/Symbols referenced:
  - [restore_toc_entry](../r/restore_toc_entry.md)
  - [TocEntry](../T/TocEntry.md)
- Called from (representative examples):
  - [_WorkerJobRestoreCustom](../W/_WorkerJobRestoreCustom.md)
  - [_WorkerJobRestoreDirectory](../W/_WorkerJobRestoreDirectory.md)

## Notes and Other Information
- Executes in worker context, not the main leader process
- Resets error count to zero before processing to isolate errors to the current TOC entry
- Requires an active database connection (AH->connection != NULL)
- Returns the status code from restore_toc_entry for error handling by the leader
- Part of the parallel processing framework that allows multiple TOC entries to be restored concurrently
- Workers report completion back to leader process which then assigns new work items