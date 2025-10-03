# lookup_filehash_entry

## Location
[src/bin/pg_rewind/filemap.c:233-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L233-L241)

## Overview
Performs a hash table lookup to find an existing file entry for a given file path without creating a new entry.

## Definition

```c
static file_entry_t *
lookup_filehash_entry(const char *path)
```
## Detailed Description
The  function provides a simple wrapper around the hash table lookup functionality. Unlike , this function only searches for existing entries and does not create new ones if the path is not found. It returns a pointer to the existing file entry if found, or NULL if the path doesn't exist in the hash table.

This function is typically used when you need to check if a file entry already exists or to retrieve information about a file that should have been previously processed and added to the hash table during the file scanning phase of pg_rewind operations.

## Parameters / Member Variables
- `*path`: The file path relative to the data directory root to look up in the hash table
## Dependencies
- Functions called/Symbols referenced:
  - filehash_lookup
  - [file_entry_t](../f/file_entry_t.md) (return type)
- Called from (representative examples):
  - [process_target_wal_block_change](../p/process_target_wal_block_change.md) (src/bin/pg_rewind/filemap.c:364)

## Notes and Other Information
- This is a static function, only accessible within filemap.c
- Returns NULL if no entry exists for the given path
- Does not modify the hash table or create new entries
- Provides read-only access to existing file entries
- Commonly used during WAL block change processing to locate files that need page-level modifications