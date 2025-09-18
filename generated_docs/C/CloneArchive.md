# CloneArchive

## Location
src/bin/pg_dump/pg_backup_archiver.c: 4978 - 5041

## Overview
Creates a clone of an ArchiveHandle structure for use in parallel restoration processes, ensuring that each worker thread has its own isolated copy with independent connection and state management.

## Definition


## Detailed Description
CloneArchive creates a deep clone of an ArchiveHandle for parallel processing in pg_dump/pg_restore operations. The function performs a "flat" copy of the main structure and then selectively clones or resets specific fields to ensure thread safety. Each clone gets its own database connection, error tracking, and transaction management settings. The cloned archive is immediately connected to the database using the same connection parameters as the original, and format-specific cloning is performed through the ClonePtr function pointer.

## Parameters / Member Variables
- : The source ArchiveHandle to be cloned

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc
  - RestoreOptions
  - ConnectDatabase
  - archModeRead
  - _doSetFixedOutputState
- Called from (representative examples):
  - RunWorker (from src/bin/pg_dump/parallel.c:846)
  - ARCHIVE_OPTS (from src/bin/pg_dump/pg_backup_archiver.h:421)

## Notes and Other Information
- The clone gets its own database connection to avoid conflicts between parallel workers
- Connection state fields (connection, connCancel, currUser, etc.) are reset to NULL and re-established
- Transaction size is set to 0 for clones to ensure immediate visibility of results to other workers
- savedPassword is duplicated if present to allow independent connection management
- Error counting is reset to 0 for each clone
- Format-specific cloning is delegated to the ClonePtr function pointer
- Used primarily in parallel restoration to ensure each worker thread operates independently