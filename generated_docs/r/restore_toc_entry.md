# restore_toc_entry

## Location
src/bin/pg_dump/pg_backup_archiver.c: 834 - 1089

## Overview
The restore_toc_entry function is the core function responsible for restoring a single TOC (Table of Contents) item during PostgreSQL backup restoration, supporting both parallel and non-parallel restore modes.

## Definition


## Detailed Description
This function handles the complete restoration of a single TOC entry, which can include both schema (DDL) and data components. It manages transaction boundaries, error handling, parallel processing coordination, and various restoration optimizations. The function processes two main phases:

1. **Schema Phase (REQ_SCHEMA)**: Creates database objects like tables, indexes, functions, etc. For TABLE objects, it tracks creation success/failure to coordinate with data loading.

2. **Data Phase (REQ_DATA)**: Loads actual data into tables, handles BLOB data, and executes data-related statements. It includes optimizations like TRUNCATE before COPY in parallel mode for better performance.

The function includes special handling for database objects, trigger management, transaction size limits, and coordination between parallel worker processes and the parent process through return status codes.

## Parameters / Member Variables
- `AH`: Archive handle containing restoration context and connection information
- `te`: TOC entry to be restored, containing object definition, data, and metadata
- `is_parallel`: Boolean flag indicating if running in a parallel worker process

## Dependencies
- Functions called/Symbols referenced:
  - CommitTransaction
  - StartTransaction
  - inhibit_data_for_failed_table
  - mark_create_done
  - _disableTriggersIfNecessary
  - _enableTriggersIfNecessary
  - is_load_via_partition_root
  - _printTocEntry
  - _reconnectToDB
  - _becomeOwner
  - _selectOutputSchema
  - pg_log_info
  - pg_log_warning
  - ahprintf
  - EndDBCopyMode
  - RestoringToDB
  - fmtQualifiedId
- Called from (representative examples):
  - RestoreArchive
  - parallel_restore
  - restore_toc_entries_prefork
  - restore_toc_entries_postfork

## Notes and Other Information
- Returns status codes: WORKER_OK (normal), WORKER_CREATE_DONE (table created successfully), WORKER_INHIBIT_DATA (table creation failed), or WORKER_IGNORED_ERRORS (errors occurred)
- Supports transaction-size mode for batching operations into transactions
- Handles special cases for DATABASE and BLOB objects with different processing logic
- Includes performance optimization for parallel restore using TRUNCATE before COPY when safe
- Coordinates with parallel parent process through return status codes rather than direct state modification
- Manages trigger disable/enable around data loading for performance
- Tracks current TOC entry in AH->currentTE for error reporting context