# process_source_file

## Location
src/bin/pg_rewind/filemap.c: 279 - 314

## Overview
A callback function that processes each file found in the source PostgreSQL server during pg_rewind operations, recording file metadata for later comparison and action determination.

## Definition
void process_source_file(const char *path, file_type_t type, size_t size, const char *link_target)

## Detailed Description
This function serves as a callback that is invoked once for every file discovered in the source PostgreSQL server during the file scanning phase of pg_rewind. It records essential metadata about each source file (type, size, symlink target) in the global filehash table, which will later be used by decide_file_action() to determine what actions need to be taken for each file during the rewind process.

The function performs several important tasks:
1. Normalizes pg_wal symlinks to be treated as directories to avoid symlink-related complications
2. Validates that files that appear to be relation data files are actually regular files
3. Stores the file metadata in the filehash table for later processing
4. Prevents duplicate entries by checking if a source file has already been processed

This function is part of the larger pg_rewind workflow where files from both source and target systems are cataloged before determining the minimal set of changes needed to rewind the target database.

## Parameters / Member Variables
- `path`: The file path relative to the PostgreSQL data directory being processed
- `type`: The file type (regular file, directory, symlink, etc.) as defined by file_type_t enum
- `size`: The size of the file in bytes (relevant for regular files)
- `link_target`: The target path for symlinks, or NULL for non-symlink files

## Dependencies
- Functions called/Symbols referenced:
  - file_type_t (enum type)
  - [file_entry_t](../f/file_entry_t.md) (structure type)
  - FILE_TYPE_SYMLINK (enum value)
  - FILE_TYPE_DIRECTORY (enum value)
  - FILE_TYPE_REGULAR (enum value)
  - [isRelDataFile](../i/isRelDataFile.md) (function to check if path is a relation data file)
  - [insert_filehash_entry](../i/insert_filehash_entry.md) (function to insert or find filehash entry)
  - [pg_fatal](pg_fatal.md) (error reporting function)
  - [pg_strdup](pg_strdup.md) (string duplication function)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_rewind.c:474)

## Notes and Other Information
- This is a public function that can be used as a callback by file traversal routines
- Special handling for pg_wal directory: symlinks are treated as regular directories to avoid complications during rewind
- Includes validation to ensure relation data files are regular files, failing with pg_fatal if they are not
- Prevents duplicate processing by checking the source_exists flag in the file entry
- The link_target parameter is duplicated with pg_strdup() when not NULL to ensure proper memory management
- This function is typically called during the source file enumeration phase of pg_rewind, before any file actions are decided or executed