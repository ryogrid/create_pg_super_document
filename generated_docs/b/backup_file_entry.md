# backup_file_entry

## Location
[src/backend/backup/basebackup_incremental.c:62-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L62-L64)

## Overview
A structure that holds file information extracted from the file list present in a backup manifest, used for tracking files during incremental backup operations.

## Definition
```c
typedef struct
{
    uint32      status;
    const char *path;
    size_t      size;
} backup_file_entry;
```

## Detailed Description
The backup_file_entry structure is used to store metadata about individual files found in backup manifests during incremental backup processing. Each entry represents a single file with its path, size, and status information. This structure serves as an element type for hash tables that efficiently track and lookup files during backup operations. The structure is designed to work with PostgreSQL's simplehash implementation for fast file lookups by path.

## Parameters / Member Variables
- `status`: Status flags or information about the file entry
- `path`: File path string (const char pointer to the file's path)
- `size`: Size of the file in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [hash_string_pointer](../h/hash_string_pointer.md) (used for hash table key generation)
- Called from (representative examples):
  - [manifest_process_file](../m/manifest_process_file.md) (creates and populates instances)
  - backup_file_insert (hash table operations)

## Notes and Other Information
- This structure is used as SH_ELEMENT_TYPE in the simplehash hash table implementation for efficient file lookups
- The path member is used as the hash key (SH_KEY) for the hash table operations
- Memory for the path string is typically allocated using MemoryContextStrdup() to ensure proper memory management
- The structure is primarily used in incremental backup functionality to track which files are already present in previous backups
- Located in src/backend/backup/basebackup_incremental.c:57-61