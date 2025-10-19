# process_target_file

## Location
[src/bin/pg_rewind/filemap.c:315-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L315-L351)

## Overview
A callback function that processes each file found in the target PostgreSQL server during pg_rewind operations, recording file metadata for later comparison with corresponding source files.

## Definition
void process_target_file(const char *path, file_type_t type, size_t size, const char *link_target)

## Detailed Description
This function serves as a callback that is invoked once for every file discovered in the target PostgreSQL server during the file scanning phase of pg_rewind. It records essential metadata about each target file (type, size, symlink target) in the global filehash table, similar to what process_source_file() does for source files. The information collected will later be used by decide_file_action() to determine what actions need to be taken for each file during the rewind process.

Key characteristics of this function:
1. Does not apply any exclusion filters, allowing complete enumeration of target files
2. Normalizes pg_wal symlinks to be treated as directories, consistent with source processing
3. Stores target file metadata in the same filehash table used for source files
4. Prevents duplicate entries by checking if a target file has already been processed

The function is part of the dual-phase file discovery process where both source and target files are catalogued before pg_rewind can determine the minimal set of changes needed to synchronize the target database with the source.

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
  - [insert_filehash_entry](../i/insert_filehash_entry.md) (function to insert or find filehash entry)
  - [pg_fatal](pg_fatal.md) (error reporting function)
  - [pg_strdup](pg_strdup.md) (string duplication function)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_rewind.c:478)

## Notes and Other Information
- This is a public function that can be used as a callback by file traversal routines
- Deliberately does not apply exclusion filters during target file processing to ensure complete enumeration
- Contains a comment noting an error message bug: says 'duplicate source file' when it should say 'duplicate target file'
- Special handling for pg_wal directory: symlinks are treated as regular directories, consistent with process_source_file behavior
- The link_target parameter is duplicated with pg_strdup() when not NULL to ensure proper memory management
- Unlike process_source_file, this function does not perform relation data file validation
- This function is typically called during the target file enumeration phase of pg_rewind, before any file actions are decided or executed
- Works in conjunction with process_source_file to build a complete picture of files in both source and target systems

## Simplified Source

```c
void
process_target_file(const char *path, file_type_t type, size_t size,
                   const char *link_target)
{
    file_entry_t *entry;

    // Treat pg_wal symlinks as directories
    if (strcmp(path, "pg_wal") == 0 && type == FILE_TYPE_SYMLINK)
        type = FILE_TYPE_DIRECTORY;

    // Store target file metadata
    entry = insert_filehash_entry(path);
    if (entry->target_exists)
        pg_fatal("duplicate source file \"%s\"", path);

    entry->target_exists = true;
    entry->target_type = type;
    entry->target_size = size;
    entry->target_link_target = link_target ? pg_strdup(link_target) : NULL;
}
```