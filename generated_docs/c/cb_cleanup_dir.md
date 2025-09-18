# cb_cleanup_dir

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 50 - 55

## Overview
A structure used to track directories that need cleanup (removal or content removal) if the pg_combinebackup operation fails.

## Definition


## Detailed Description
The  structure is part of pg_combinebackup's error handling mechanism. It maintains a linked list of directories that should be cleaned up if the backup combination operation fails or is interrupted. This ensures that partial operations don't leave behind corrupted or incomplete directory structures.

The structure forms a singly-linked list where each node represents a directory that requires cleanup. The cleanup behavior is controlled by the  flag, which determines whether only the directory contents should be removed or if the top-level directory itself should also be removed.

## Parameters / Member Variables
- : Path to the directory that needs cleanup
- : Boolean flag indicating whether to remove the top directory itself (true) or just its contents (false)
- : Pointer to the next cleanup directory in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - (Self-referential structure member)
- Called from (representative examples):
  - cleanup_directories_atexit
  - remember_to_cleanup_directory
  - reset_directory_cleanup_list

## Notes and Other Information
- Part of pg_combinebackup's error handling and cleanup infrastructure
- Used to maintain a list of directories created during backup combination that should be removed if the operation fails
- The linked list structure allows for dynamic addition of directories during the backup combination process
- Essential for preventing partial backup states from persisting after failed operations