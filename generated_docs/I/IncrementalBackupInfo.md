# IncrementalBackupInfo

## Location
src/backend/backup/basebackup_incremental.c: 76 - 143

## Overview
IncrementalBackupInfo is a structure that manages the state and metadata required for performing incremental backups in PostgreSQL, storing information from backup manifests and tracking block-level changes.

## Definition


## Detailed Description
IncrementalBackupInfo serves as the central data structure for managing incremental backup operations in PostgreSQL. It maintains all the necessary state information to perform efficient incremental backups by tracking which blocks have been modified since the previous backup. The structure is designed to handle the complex task of parsing backup manifests, managing WAL ranges, and maintaining block-level change tracking for optimal backup performance.

The structure leverages WAL summaries to determine what has changed rather than relying solely on file lists, which provides better safety against scenarios where files are removed and recreated with the same name but different contents. The block-reference table uses an efficient in-memory format that converges to approximately 1 bit per block for relation forks with large numbers of modified blocks.

## Parameters / Member Variables
- : Memory context that manages memory allocation for this object and all its subsidiary objects, ensuring proper cleanup
- : Temporary StringInfo buffer used for storing and parsing backup manifest data during processing
- : List containing WAL ranges extracted from the backup manifest, used to determine the scope of changes
- : Hash table of files from the previous backup manifest, used for sanity checking but not primary change detection
- : Block-reference table that tracks which specific blocks need to be included in the incremental backup
- : State object that maintains context during incremental JSON parsing of backup manifests

## Dependencies
- Functions called/Symbols referenced:
  - BlockRefTable
  - JsonManifestParseIncrementalState
  - manifest_process_version
  - manifest_process_system_identifier
  - manifest_process_file
  - manifest_process_wal_range
  - JsonManifestParseContext
  - pg_checksum_type
- Called from (representative examples):
  - perform_base_backup
  - SendBaseBackup
  - PrepareForIncrementalBackup
  - GetFileBackupMethod
  - AppendIncrementalManifestData
  - FinalizeIncrementalManifest

## Notes and Other Information
The structure is designed with memory efficiency in mind, but the authors acknowledge that storing the entire block-reference table in memory could potentially be problematic for very large databases on memory-constrained systems. The current implementation is optimized for most common use cases.

The manifest_files member is retained primarily for sanity checking purposes rather than primary change detection logic. While it consumes additional memory, it provides valuable validation that files expected to be unchanged actually existed in the previous backup.

The structure is located in src/backend/backup/basebackup_incremental.c:76-143 and is integral to PostgreSQL's incremental backup functionality introduced for efficient backup operations.