# reset_directory_cleanup_list

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:1226-1244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L1226-L1244)

## Overview
Empties the linked list of directories scheduled for cleanup at exit, freeing associated memory and preventing automatic directory removal upon successful program completion.

## Definition
```c
static void reset_directory_cleanup_list(void)
```

## Detailed Description
This function iterates through the cleanup_dir_list linked list and frees all cb_cleanup_dir structures, effectively clearing the list of directories scheduled for removal. It is designed to be called when the backup operation has completed successfully, preventing the cleanup mechanism from removing output directories that should be preserved. The function ensures proper memory management by freeing each node in the linked list, though this is primarily for tidiness since it's typically called before program exit.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [cb_cleanup_dir](../c/cb_cleanup_dir.md) (structure type for cleanup directory entries)
  - [cb_tablespace](../c/cb_tablespace.md) (structure type referenced in the source)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_combinebackup/pg_combinebackup.c:428)

## Notes and Other Information
- This is a static function used specifically within pg_combinebackup utility
- Called upon successful completion of backup operations to prevent directory cleanup
- Implements proper memory management by freeing all allocated cb_cleanup_dir structures
- Works as the counterpart to remember_to_cleanup_directory function
- Part of error handling and cleanup management system
- The cleanup_dir_list is set to NULL after all entries are processed and freed
- File location: src/bin/pg_combinebackup/pg_combinebackup.c:1226-1244