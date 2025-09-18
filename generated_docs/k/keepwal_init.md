# keepwal_init

## Location
[src/bin/pg_rewind/filemap.c:242-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L242-L249)

## Overview
Initializes a separate hash table used to track WAL files that must be kept during pg_rewind operations.

## Definition


## Detailed Description
The  function creates and initializes a dedicated hash table () specifically for tracking Write-Ahead Log (WAL) files that must not be deleted during the pg_rewind process. This hash table is separate from the main file map hash table and serves a specialized purpose in WAL file management.

The function uses the same simplehash library infrastructure as the main file hash table but is configured specifically for storing  structures. The hash table uses file paths as keys and is initialized with a predefined size to efficiently manage WAL file tracking during the rewind operation.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - keepwal_create
  - KEEPWAL_INITIAL_SIZE (constant set to 1000)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_rewind/pg_rewind.c:459)

## Notes and Other Information
- Initializes a separate hash table specifically for WAL file tracking
- The initial size is set to 1000 entries (KEEPWAL_INITIAL_SIZE)  
- Uses the simplehash library with string keys (file paths)
- The global  variable is used throughout the module for WAL file management
- Essential for ensuring critical WAL files are preserved during rewind operations
- Must be called before any keepwal operations are performed
- Works alongside the main file hash table but serves a distinct purpose in WAL file preservation