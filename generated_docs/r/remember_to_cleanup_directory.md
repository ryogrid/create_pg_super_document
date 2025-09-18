# remember_to_cleanup_directory

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 1205 - 1225

## Overview
Adds a directory to the cleanup list for automatic removal when the program exits or encounters an error, providing cleanup management for temporary directories created during backup operations.

## Definition
```c
static void remember_to_cleanup_directory(char *target_path, bool rmtopdir)
```

## Detailed Description
This function creates a new cleanup directory entry and adds it to the front of a linked list (cleanup_dir_list) that tracks directories scheduled for cleanup. The function allocates memory for a cb_cleanup_dir structure and initializes it with the provided directory path and removal flag. This is part of a cleanup management system that ensures temporary directories are properly removed even if the program terminates unexpectedly.

## Parameters / Member Variables
- `target_path`: Path to the directory that should be remembered for cleanup
- `rmtopdir`: Boolean flag indicating whether the top-level directory itself should be removed (true) or just its contents (false)

## Dependencies
- Functions called/Symbols referenced:
  - cb_cleanup_dir (structure type for cleanup directory entries)
  - pg_malloc (PostgreSQL memory allocation function)
- Called from (representative examples):
  - create_output_directory (in src/bin/pg_combinebackup/pg_combinebackup.c:731)
  - create_output_directory (in src/bin/pg_combinebackup/pg_combinebackup.c:736)

## Notes and Other Information
- This is a static function used specifically within pg_combinebackup utility
- Implements a simple linked list for tracking cleanup directories
- New entries are added to the front of the list (LIFO order)
- Part of error handling and cleanup management system
- Works in conjunction with cleanup routines that process the cleanup_dir_list
- Memory for the cb_cleanup_dir structure is allocated but not freed by this function
- File location: src/bin/pg_combinebackup/pg_combinebackup.c:1205-1225