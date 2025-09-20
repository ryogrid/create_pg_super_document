# ParallelBackupEnd

## Location
[src/bin/pg_dump/parallel.c:1059-1107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1059-L1107)

## Overview
Cleanly shuts down parallel backup/restore operations by terminating worker processes/threads and releasing allocated resources.

## Definition

```c
void
ParallelBackupEnd(ArchiveHandle *AH, ParallelState *pstate)
```
## Detailed Description
ParallelBackupEnd handles the orderly shutdown of parallel processing infrastructure created by ParallelBackupStart. The function ensures all workers have completed their tasks, closes communication channels to signal workers to exit, waits for worker termination, and performs cleanup of allocated resources. For single-worker scenarios, it returns immediately without performing any cleanup operations. The function also unlinks the parallel state from global shutdown and signal handling structures to prevent use-after-free issues.

## Parameters / Member Variables
- : Archive handle (not actively used but maintained for API consistency)
- : Parallel state structure containing worker information and communication channels to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [IsEveryWorkerIdle](../I/IsEveryWorkerIdle.md) (verification that all workers are finished)
  - closesocket (close communication pipes)
  - [WaitForTerminatingWorkers](../W/WaitForTerminatingWorkers.md) (wait for worker process/thread termination)
  - [set_cancel_pstate](../s/set_cancel_pstate.md) (unlink from signal handling)
  - free (memory deallocation)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md) (src/bin/pg_dump/pg_backup_archiver.c:733)
  - [_CloseArchive](../C/_CloseArchive.md) (src/bin/pg_dump/pg_backup_directory.c:598)

## Notes and Other Information
- Must be called after all parallel work is complete (asserts that IsEveryWorkerIdle is true)
- Closes communication pipes to signal workers to exit gracefully
- Waits for all worker processes/threads to terminate before returning
- Performs cleanup of global state to prevent shutdown handler conflicts
- For numWorkers == 1, performs no operations and returns immediately
- Memory cleanup includes freeing the TocEntry array, ParallelSlot array, and ParallelState structure itself