# ResetUnloggedRelationsInDbspaceDir

## Location
[src/backend/storage/file/reinit.c:161-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/reinit.c#L161-L379)

## Overview
ResetUnloggedRelationsInDbspaceDir is the core function that processes unlogged relations within a specific database directory, performing both cleanup and initialization operations on relation files with advanced hash-based tracking for efficiency.

## Definition
```c
static void ResetUnloggedRelationsInDbspaceDir(const char *dbspacedirname, int op)
```

## Detailed Description
This function is the workhorse of the unlogged relation reset system, operating at the database directory level. It performs two distinct operations based on the operation flags:

### Cleanup Operation (UNLOGGED_RELATION_CLEANUP)
Implements a sophisticated two-pass cleanup algorithm:
1. **First Pass**: Scans the directory to identify all relations with init forks, storing their RelFileNumbers in a hash table for O(1) lookup performance
2. **Second Pass**: Removes all non-init fork files that correspond to relations found in the hash table

The hash-based approach ensures O(n) performance rather than O(n²) when dealing with many unlogged relations in the same database.

### Initialization Operation (UNLOGGED_RELATION_INIT)
Restores unlogged relations to their initial state through a three-phase process:
1. **Copy Phase**: Copies init fork files to their corresponding main fork files
2. **Sync Phase**: Performs fsync on all newly created main fork files to ensure durability
3. **Directory Sync**: Syncs the database directory itself to ensure filesystem metadata persistence

## Parameters / Member Variables
- `dbspacedirname`: Path to the database directory within a tablespace (e.g., "base/16384", "pg_tblspc/16385/PG_17_6/16384")
- `op`: Bitwise operation flags specifying which operations to perform
  - `UNLOGGED_RELATION_CLEANUP` (0x0001): Enable cleanup of relation forks  
  - `UNLOGGED_RELATION_INIT` (0x0002): Enable initialization from init forks

## Dependencies
- Functions called/Symbols referenced:
  - `parse_filename_for_nontemp_relation`: Parses relation filenames to extract components
  - `hash_create`/`hash_search`/`hash_destroy`: Hash table operations for RelFileNumber tracking
  - `AllocateDir`/`ReadDir`/`FreeDir`: Directory traversal operations
  - `copy_file`: File copying operation for init-to-main fork copying
  - `fsync_fname`: File synchronization for durability
  - `unlink`: File deletion during cleanup
  - `ereport`/`elog`: Error reporting and debug logging

- Data structures:
  - `unlogged_relation_entry`: Hash table entry containing RelFileNumber as key
  - `HTAB`: Hash table for tracking relations with init forks

- Called from:
  - `ResetUnloggedRelationsInTablespaceDir`: For each database directory (line 151)

## Notes and Other Information
- This is a static function, internal to the reinit.c module
- Uses efficient hash table to avoid O(n²) performance when many unlogged relations exist
- The cleanup phase is optimized to early-exit if no init forks are found
- File copying includes proper error handling and debug logging
- The sync phase is separated to allow kernel to optimize metadata operations
- Directory fsync ensures filesystem consistency after file operations
- The function handles relation file segments (e.g., relation.1, relation.2) correctly
- Initialization always happens after cleanup to ensure proper ordering
- Located in src/backend/storage/file/reinit.c:161-379