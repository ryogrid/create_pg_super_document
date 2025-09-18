# keepwal_entry_exists

## Location
[src/bin/pg_rewind/filemap.c:266-278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L266-L278)

## Overview
A utility function that checks whether a given file path exists in the keepwal hash table, which tracks WAL files that must not be deleted during pg_rewind operations.

## Definition
static bool keepwal_entry_exists(const char *path)

## Detailed Description
This function serves as a convenience wrapper around the keepwal_lookup() function to determine if a specific WAL file path is marked for preservation. The function returns a boolean value indicating whether the file should be kept (not removed) during the rewind process. It operates on the global keepwal hash table which maintains a list of WAL files that are essential for the rewind operation and must be preserved.

The function is part of pg_rewind's file management system that categorizes files into different actions (copy, remove, keep, etc.) based on their importance and state differences between source and target PostgreSQL instances.

## Parameters / Member Variables
- `path`: A string containing the file path to check in the keepwal hash table. This should be a relative path within the PostgreSQL data directory.

## Dependencies
- Functions called/Symbols referenced:
  - keepwal_lookup (from keepwal hash table implementation)
- Called from (representative examples):
  - [decide_file_action](../d/decide_file_action.md) (in filemap.c:754)

## Notes and Other Information
- This is a static function, only accessible within the filemap.c file
- The function relies on the keepwal global hash table (keepwal_hash type) that must be initialized before use
- Returns true if the file exists in the keepwal table, false otherwise
- The keepwal hash table uses a simple hash implementation with initial size of KEEPWAL_INITIAL_SIZE (1000)
- This function is critical for ensuring that essential WAL files are not accidentally removed during the rewind process