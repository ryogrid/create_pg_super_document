# mark_restore_job_done

## Location
src/bin/pg_dump/pg_backup_archiver.c: 4634 - 4674

## Overview
Callback function invoked in the leader process after a parallel worker completes restoring a TOC entry.

## Definition
static void mark_restore_job_done(ArchiveHandle *AH, TocEntry *te, int status, void *callback_data)

## Detailed Description
This callback function is executed in the leader process when a parallel worker reports completion of a restore job. It handles the post-processing tasks including status updates, error counting, and dependency management. Based on the worker's completion status, it takes appropriate actions such as marking items as created, handling data inhibition for failed tables, or counting errors.

After processing the completion status, the function calls reduce_dependencies to update dependency counts for items that were waiting on the completed entry, potentially making new items ready for execution.

## Parameters / Member Variables
- AH: Archive handle containing restore context and error counters
- te: TOC entry that was just completed by a worker
- status: Completion status returned by the worker (0 for success, various error codes for different failure types)
- callback_data: Binary heap passed as callback data to receive newly ready items

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - [mark_create_done](mark_create_done.md)
  - [inhibit_data_for_failed_table](../i/inhibit_data_for_failed_table.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [reduce_dependencies](../r/reduce_dependencies.md)
  - [TocEntry](../T/TocEntry.md)
  - [binaryheap](../b/binaryheap.md)
  - WORKER_CREATE_DONE
  - WORKER_INHIBIT_DATA
  - WORKER_IGNORED_ERRORS
- Called from (representative examples):
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md)

## Notes and Other Information
- Logs completion information including dump ID, description, and tag of the completed item
- Handles different completion statuses: successful completion, data inhibition, ignored errors, and fatal errors
- Increments the global error counter for certain types of failures
- Calls pg_fatal for unexpected worker exit codes, terminating the entire restore process
- The reduce_dependencies call may add new items to the ready heap as their dependencies are satisfied