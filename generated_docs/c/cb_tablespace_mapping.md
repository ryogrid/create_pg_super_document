# cb_tablespace_mapping

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 60 - 65

## Overview
A structure that stores tablespace directory mappings provided via the -T/--tablespace-mapping command line option in pg_combinebackup.

## Definition


## Detailed Description
The  structure is used by pg_combinebackup to handle tablespace directory remapping during backup combination operations. When users specify the -T or --tablespace-mapping option, each mapping is stored in this structure as part of a linked list. This allows the tool to relocate tablespaces from their original backup locations to new target directories during the combination process.

The structure forms a singly-linked list where each node represents a single tablespace mapping from an old directory path to a new directory path. This is essential for scenarios where the combined backup needs to be restored to a system with different tablespace locations than the original.

## Parameters / Member Variables
- : Original tablespace directory path from the backup (maximum MAXPGPATH characters)
- : Target directory path where the tablespace should be relocated (maximum MAXPGPATH characters)
- : Pointer to the next tablespace mapping in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - MAXPGPATH (constant for maximum path length)
  - (Self-referential structure member)
- Called from (representative examples):
  - [cb_options](cb_options.md) (as a member)
  - [add_tablespace_mapping](../a/add_tablespace_mapping.md)
  - [scan_for_existing_tablespaces](../s/scan_for_existing_tablespaces.md)

## Notes and Other Information
- Part of pg_combinebackup's tablespace relocation functionality
- Used to implement the -T/--tablespace-mapping command line option
- The linked list structure allows for multiple tablespace mappings to be specified
- [Path](../P/Path.md) lengths are constrained by MAXPGPATH to ensure compatibility with PostgreSQL's path handling
- Essential for cross-system backup restoration where tablespace paths may differ