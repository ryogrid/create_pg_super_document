# bbsink_progress_new

## Location
[src/backend/backup/basebackup_progress.c:59-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_progress.c#L59-L83)

## Overview
Creates a new basebackup sink that performs progress tracking functions and forwards data to a successor sink in the PostgreSQL base backup system.

## Definition
bbsink *bbsink_progress_new(bbsink *next, bool estimate_backup_size)

## Detailed Description
This function creates a new basebackup sink wrapper that adds progress tracking capabilities to an existing sink. It initializes the progress reporting system for base backup operations by starting a progress command and setting up initial parameters. The function allocates a new bbsink structure, sets up the progress-specific operations table, and chains it to the next sink in the pipeline. It reports the start of a base backup operation to the PostgreSQL statistics system with an initial total size of -1 (which translates to NULL), indicating that the backup size is not yet known.

## Parameters / Member Variables
- : The successor sink in the chain that will receive forwarded data (must not be NULL)
- : Boolean flag indicating whether backup size estimation should be performed

## Dependencies
- Functions called/Symbols referenced:
  - [bbsink](bbsink.md) (structure type)
  - [bbsink_ops](bbsink_ops.md) (operations structure)
  - [palloc0](../p/palloc0.md) (memory allocation)
  - [pgstat_progress_start_command](../p/pgstat_progress_start_command.md) (progress reporting)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md) (progress parameter update)
  - PROGRESS_COMMAND_BASEBACKUP (progress command constant)
  - PROGRESS_BASEBACKUP_BACKUP_TOTAL (progress parameter constant)
- Called from (representative examples):
  - [SendBaseBackup](../S/SendBaseBackup.md) (in basebackup.c)
  - [bbsink_cleanup](bbsink_cleanup.md) (referenced in basebackup_sink.h)

## Notes and Other Information
- The function uses palloc0 to allocate zero-initialized memory for the sink structure
- Progress tracking is initialized with a total backup size of -1, which gets translated to NULL in the progress reporting system
- The actual backup size estimate will be updated later if estimate_backup_size is true
- This function is part of the PostgreSQL base backup infrastructure that provides real-time progress information during backup operations

## Simplified Source

```c
// Simplified version of bbsink_progress_new
bbsink *bbsink_progress_new(bbsink *next, bool estimate_backup_size) {
    bbsink *sink;

    // Validate next sink exists
    Assert(next != NULL);

    // Allocate and initialize progress tracking sink
    sink = palloc0(sizeof(bbsink));

    // Set up operations table for progress tracking functionality
    *((const bbsink_ops **) &sink->bbs_ops) = &bbsink_progress_ops;

    // Chain to next sink in pipeline
    sink->bbs_next = next;

    // Initialize progress reporting for base backup
    // Start command tracking with invalid OID
    pgstat_progress_start_command(PROGRESS_COMMAND_BASEBACKUP, InvalidOid);

    // Set total backup size to -1 (translates to NULL = unknown size)
    // Will be updated later if size estimation is enabled
    pgstat_progress_update_param(PROGRESS_BASEBACKUP_BACKUP_TOTAL, -1);

    return sink;
}
```

Key simplifications made:
- Added clear comments explaining progress tracking initialization
- Preserved all essential functionality and sink chaining
- Maintained progress reporting system setup
- Kept validation and memory allocation intact
- Simplified structure while preserving all functionality
- Note: estimate_backup_size parameter is stored implicitly in the progress system