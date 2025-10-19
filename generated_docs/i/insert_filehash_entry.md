# insert_filehash_entry

## Location
[src/bin/pg_rewind/filemap.c:203-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L203-L232)

## Overview
Looks up or creates a new file entry in the hash table for a given file path during pg_rewind operations.

## Definition

```c
static file_entry_t *
insert_filehash_entry(const char *path)
```
## Detailed Description
The  function serves as a hash table insertion mechanism that either retrieves an existing file entry or creates a new one for the specified file path. When a new entry is created, the function initializes all fields of the  structure with default values, preparing it for subsequent processing during the pg_rewind operation.

The function uses the simplehash library's insertion mechanism and properly initializes new entries with sensible defaults: file existence flags are set to false, file types are set to undefined, sizes are zeroed, and the action is set to undecided. The function also determines whether the file is a relation data file and duplicates the path string for safe storage.

## Parameters / Member Variables
- `*path`: The file path relative to the data directory root for which to create or retrieve a hash table entry
## Dependencies
- Functions called/Symbols referenced:
  - filehash_insert
  - [pg_strdup](../p/pg_strdup.md)
  - [isRelDataFile](isRelDataFile.md)
  - FILE_TYPE_UNDEFINED
  - FILE_ACTION_UNDECIDED
  - [file_entry_t](../f/file_entry_t.md) (structure type)
- Called from (representative examples):
  - [process_source_file](../p/process_source_file.md) (src/bin/pg_rewind/filemap.c:300)
  - [process_target_file](../p/process_target_file.md) (src/bin/pg_rewind/filemap.c:333)

## Notes and Other Information
- This is a static function, only accessible within filemap.c
- Creates a duplicate of the path string using pg_strdup for safe storage
- Initializes all target and source file attributes to default/undefined states
- The isrelfile field is set based on whether the path represents a relation data file
- Returns a pointer to the file_entry_t structure, whether newly created or existing
- Essential for building the complete file map before deciding on rewind actions

## Simplified Source

```c
static file_entry_t *insert_filehash_entry(const char *path) {
    file_entry_t *entry;
    bool found;

    // Insert or lookup entry in hash table
    entry = filehash_insert(filehash, path, &found);

    if (!found) {
        // Initialize new entry with default values
        entry->path = pg_strdup(path);
        entry->isrelfile = isRelDataFile(path);

        // Initialize target file attributes
        entry->target_exists = false;
        entry->target_type = FILE_TYPE_UNDEFINED;
        entry->target_size = 0;
        entry->target_link_target = NULL;
        entry->target_pages_to_overwrite.bitmap = NULL;
        entry->target_pages_to_overwrite.bitmapsize = 0;

        // Initialize source file attributes
        entry->source_exists = false;
        entry->source_type = FILE_TYPE_UNDEFINED;
        entry->source_size = 0;
        entry->source_link_target = NULL;

        // Mark action as undecided
        entry->action = FILE_ACTION_UNDECIDED;
    }

    return entry;
}
```