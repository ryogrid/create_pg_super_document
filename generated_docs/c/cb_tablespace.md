# cb_tablespace

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 90 - 97

## Overview
A structure that represents tablespace information during pg_combinebackup operations, including both normal and in-place tablespaces.

## Definition


## Detailed Description
The  structure stores comprehensive information about tablespaces discovered during backup combination operations. It distinguishes between normal tablespaces that require directory mapping and in-place tablespaces that can remain in their original locations. This structure forms a linked list containing all tablespaces found in the backup sets being combined.

The structure handles two types of tablespaces: normal tablespaces that need explicit directory mappings (provided via command-line options) and in-place tablespaces that don't require relocation. This distinction is crucial for pg_combinebackup to properly handle different tablespace scenarios during backup combination.

## Parameters / Member Variables
- : PostgreSQL object identifier (OID) of the tablespace
- : Boolean flag indicating if this is an in-place tablespace (doesn't need mapping)
- : Original directory path of the tablespace from the backup (maximum MAXPGPATH characters)
- : Target directory path where the tablespace should be placed (maximum MAXPGPATH characters)
- : Pointer to the next tablespace in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - MAXPGPATH (constant for maximum path length)
  - (Self-referential structure member)
- Called from (representative examples):
  - [main](../m/main.md)
  - [reset_directory_cleanup_list](../r/reset_directory_cleanup_list.md)
  - [scan_for_existing_tablespaces](../s/scan_for_existing_tablespaces.md)

## Notes and Other Information
- Part of pg_combinebackup's tablespace discovery and management system
- Can contain more entries than tablespace mappings since in-place tablespaces don't require explicit mappings
- The linked list structure accommodates multiple tablespaces across different backup sets
- Essential for handling mixed scenarios with both relocatable and in-place tablespaces
- The in_place flag optimizes processing by skipping unnecessary mapping operations for certain tablespaces
- Used in conjunction with cb_tablespace_mapping to provide complete tablespace handling capability