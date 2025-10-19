# keepwal_add_entry

## Location
[src/bin/pg_rewind/filemap.c:250-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L250-L265)

## Overview
Adds a WAL file path to the keep-list hash table to prevent its removal during pg_rewind operations.

## Definition

```c
void
keepwal_add_entry(const char *path)
```
## Detailed Description
The  function marks a specified WAL file path for preservation by adding it to the dedicated keepwal hash table. This function ensures that critical WAL files identified during the rewind process are not deleted, as they may be necessary for maintaining database consistency or for recovery purposes.

The function performs an insertion operation similar to the main file hash table but specifically for WAL files. If the entry doesn't already exist, it creates a new entry and duplicates the path string for safe storage. The function includes an assertion to ensure the keepwal hash table has been properly initialized before use.

## Parameters / Member Variables
- `*path`: The WAL file path to be marked for preservation during the rewind operation
## Dependencies
- Functions called/Symbols referenced:
  - keepwal_insert
  - [pg_strdup](../p/pg_strdup.md)
  - Assert (macro)
  - [keepwal_entry](keepwal_entry.md) (structure type)
- Called from (representative examples):
  - [findLastCheckpoint](../f/findLastCheckpoint.md) (src/bin/pg_rewind/parsexlog.c:238)

## Notes and Other Information
- Requires keepwal hash table to be initialized via  before use
- Creates a duplicate of the path string using pg_strdup for safe storage
- The function has an assertion that verifies keepwal is not NULL before proceeding
- Used during WAL analysis to identify files that must be preserved
- Prevents duplicate entries by checking if the path already exists
- Essential for maintaining WAL file integrity during database rewind operations
- Typically called during checkpoint analysis and WAL processing phases

## Simplified Source

```c
void
keepwal_add_entry(const char *path)
{
    keepwal_entry *entry;
    bool found;

    // Ensure keepwal is initialized
    Assert(keepwal != NULL);

    // Insert entry into keepwal hash table
    entry = keepwal_insert(keepwal, path, &found);

    // Store path if this is a new entry
    if (!found)
        entry->path = pg_strdup(path);
}
```