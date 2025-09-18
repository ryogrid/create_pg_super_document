# RestoreArchive

## Location
src/bin/pg_dump/pg_backup_archiver.c: 334 - 833

## Overview
Performs the complete restoration process of a PostgreSQL dump archive, handling both serial and parallel restore modes with comprehensive transaction management and error handling.

## Definition
```c
void RestoreArchive(Archive *AHX)
```

## Detailed Description
The RestoreArchive function is the main entry point for restoring a PostgreSQL dump archive. It orchestrates the entire restoration process through multiple stages: initialization, processing, and finalization. The function supports both serial and parallel restore modes, with extensive validation and error checking throughout.

The restoration process includes several key phases:
1. **Initialization**: Validates parallel restore compatibility, checks compression support, and establishes database connections if needed
2. **Schema Analysis**: Determines if the restore is data-only based on available TOC entries
3. **Output Setup**: Configures output files and compression as specified
4. **Drop Phase**: Optionally drops existing objects in reverse dependency order
5. **Restore Phase**: Processes TOC entries in appropriate order (serial mode uses three passes: main, ACL, post-ACL; parallel mode uses worker processes)
6. **Finalization**: Commits transactions, closes connections, and performs cleanup

The function handles various restore options including single transactions, transaction batching, parallel processing, and conditional object creation/dropping.

## Parameters / Member Variables
- `AHX`: Pointer to the Archive structure containing the dump to restore

## Dependencies
- Functions called/Symbols referenced:
  - [buildTocEntryArrays](../b/buildTocEntryArrays.md)
  - [ConnectDatabase](../C/ConnectDatabase.md)
  - [DisconnectDatabase](../D/DisconnectDatabase.md)
  - [SaveOutput](../S/SaveOutput.md)
  - [SetOutput](../S/SetOutput.md)
  - [RestoreOutput](RestoreOutput.md)
  - [StartTransaction](../S/StartTransaction.md)
  - [CommitTransaction](../C/CommitTransaction.md)
  - [_doSetFixedOutputState](../d/_doSetFixedOutputState.md)
  - [_becomeOwner](../b/_becomeOwner.md)
  - [_selectOutputSchema](../s/_selectOutputSchema.md)
  - [restore_toc_entry](../r/restore_toc_entry.md)
  - [restore_toc_entries_prefork](../r/restore_toc_entries_prefork.md)
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md)
  - [restore_toc_entries_postfork](../r/restore_toc_entries_postfork.md)
  - [ParallelBackupStart](../P/ParallelBackupStart.md)
  - [ParallelBackupEnd](../P/ParallelBackupEnd.md)
  - [IssueCommandPerBlob](../I/IssueCommandPerBlob.md)
  - [DropLOIfExists](../D/DropLOIfExists.md)
  - [supports_compression](../s/supports_compression.md)
  - [dumpTimestamp](../d/dumpTimestamp.md)
  - [ahprintf](../a/ahprintf.md)
  - pg_log_info
  - pg_log_warning
  - [pg_fatal](../p/pg_fatal.md)
  - And many constants and enums for stages, sections, and requirements
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c)
  - [main](../m/main.md) (in pg_restore.c)
  - [_CloseArchive](../C/_CloseArchive.md) (in pg_backup_tar.c)

## Notes and Other Information
- This is a public function and the primary interface for archive restoration
- Supports both serial and parallel restore modes with different processing strategies
- Handles comprehensive transaction management including single transactions and batched commits
- Provides extensive validation for parallel restore compatibility and compression support
- Manages object dropping with proper dependency ordering and IF EXISTS clause injection
- Includes sophisticated error handling and logging throughout the process
- The function spans over 500 lines, making it one of the most complex functions in the pg_dump architecture
- Critical for both standalone pg_restore operations and integrated dump-restore workflows
- Supports various output formats and compression algorithms
- Handles special cases for large objects, database properties, and constraint objects