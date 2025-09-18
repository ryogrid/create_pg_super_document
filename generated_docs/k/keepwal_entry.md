# keepwal_entry

## Location
src/bin/pg_rewind/filemap.c: 67 - 71

## Overview
The `keepwal_entry` structure is used in PostgreSQL's pg_rewind utility to track Write-Ahead Log (WAL) files that must not be deleted during the rewind process.

## Definition
```c
typedef struct keepwal_entry
{
    const char *path;
    uint32      status;
} keepwal_entry;
```

## Detailed Description
This structure serves as an element type for a hash table that maintains a list of WAL files that should be preserved during pg_rewind operations. The pg_rewind utility needs to carefully manage WAL files to ensure data consistency when rewinding a PostgreSQL server to an earlier point in time. The keepwal_entry structure is part of the simplehash.h framework implementation, providing efficient lookups for WAL file paths that need protection from deletion.

The structure is designed to work with PostgreSQL's generic hash table implementation (simplehash.h), where the path field serves as the key for hash table operations.

## Parameters / Member Variables
- `path`: A string containing the file path of the WAL file that must be preserved
- `status`: A 32-bit unsigned integer representing the status of the WAL file entry (specific usage depends on implementation context)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure definition)
- Called from (representative examples):
  - `SH_ELEMENT_TYPE` (used in simplehash.h template instantiation)
  - `keepwal_add_entry` (creates and manages entries of this type)

## Notes and Other Information
- This structure is specifically used within the pg_rewind utility's filemap.c module
- It integrates with PostgreSQL's simplehash.h generic hash table framework
- The hash table using this structure is initialized with KEEPWAL_INITIAL_SIZE (1000) entries
- WAL files tracked by this structure are critical for maintaining database consistency during rewind operations
- The structure definition is located at src/bin/pg_rewind/filemap.c:67-71