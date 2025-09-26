# WriteDataChunks

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2475-2540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2475-L2540)

## Overview
WriteDataChunks is a function that orchestrates the writing of all data chunks (tables and large objects) during a PostgreSQL dump operation, supporting both parallel and sequential execution modes.

## Definition
```c
void WriteDataChunks(ArchiveHandle *AH, ParallelState *pstate)
```

## Detailed Description
This function manages the output of all data content during a dump operation by iterating through the table of contents and processing entries that have data dumpers. In parallel mode, it creates an array of eligible TOC entries, sorts them by size in descending order to optimize parallel execution, and dispatches jobs to worker processes. In sequential mode, it processes each eligible entry directly. The function filters entries to only include those with dataDumper functions and REQ_DATA requirements enabled.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer - the archive handle containing the table of contents and configuration
- `pstate`: ParallelState pointer - parallel execution state, NULL for sequential mode

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - qsort
  - [TocEntrySizeCompareQsort](../T/TocEntrySizeCompareQsort.md)
  - [DispatchJobForTocEntry](../D/DispatchJobForTocEntry.md)
  - [mark_dump_job_done](../m/mark_dump_job_done.md)
  - [pg_free](../p/pg_free.md)
  - [WaitForWorkers](WaitForWorkers.md)
  - [WriteDataChunksForTocEntry](WriteDataChunksForTocEntry.md)
  - ACT_DUMP, REQ_DATA, WFW_ALL_IDLE (constants)
- Called from (representative examples):
  - [_CloseArchive](../C/_CloseArchive.md) (in various format handlers)

## Notes and Other Information
- Public function, declared in pg_backup_archiver.h
- Critical performance optimization through size-based sorting in parallel mode
- Ensures larger tables are dumped first to maximize parallel efficiency
- Filters TOC entries based on both dataDumper presence and REQ_DATA flag
- Part of the final phase of archive creation in pg_dump operations
- Coordinates between leader and worker processes in parallel dump scenarios
- Waits for all workers to complete before returning in parallel mode