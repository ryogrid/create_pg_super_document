# manifest_data

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 99 - 106

## Overview
A comprehensive data structure that holds all parsed information from a PostgreSQL backup manifest file, serving as the central container for backup metadata, file listings, and WAL ranges.

## Definition
```c
typedef struct manifest_data
{
    int                 version;
    uint64              system_identifier;
    manifest_files_hash *files;
    manifest_wal_range  *first_wal_range;
    manifest_wal_range  *last_wal_range;
} manifest_data;
```

## Detailed Description
The `manifest_data` structure represents the complete parsed contents of a backup manifest file in PostgreSQL. It serves as the top-level container that aggregates all backup-related metadata including the manifest version, database system identifier, hash table of files, and linked list of required WAL ranges. This structure is central to backup verification and combining operations, providing a unified view of all backup components that need to be processed or verified.

## Parameters / Member Variables
- `version`: Version number of the backup manifest format
- `system_identifier`: Unique identifier of the PostgreSQL database system that created the backup
- `files`: Hash table containing all manifest_file entries indexed by pathname
- `first_wal_range`: Pointer to the first element in the linked list of required WAL ranges
- `last_wal_range`: Pointer to the last element in the linked list of required WAL ranges

## Dependencies
- Functions called/Symbols referenced:
  - manifest_wal_range
  - manifest_files_hash (implied hash table type for files)

- Called from (representative examples):
  - load_backup_manifest (src/bin/pg_combinebackup/load_manifest.c:116)
  - parse_manifest_file (src/bin/pg_verifybackup/pg_verifybackup.c:400)
  - main (src/bin/pg_verifybackup/pg_verifybackup.c:389)
  - verifybackup_version_cb (src/bin/pg_verifybackup/pg_verifybackup.c:525)
  - process_directory_recursively (src/bin/pg_combinebackup/pg_combinebackup.c:829)

## Notes and Other Information
This structure is the primary data container used by PostgreSQL backup tools (pg_verifybackup, pg_combinebackup) to manage and process backup manifests. The system_identifier is crucial for ensuring that backups come from the correct database instance. The structure efficiently organizes files in a hash table for fast lookup while maintaining WAL ranges in a linked list for sequential processing. The version field allows for future extensibility of the manifest format.