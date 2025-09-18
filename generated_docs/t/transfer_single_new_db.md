# transfer_single_new_db

## Location
src/bin/pg_upgrade/relfilenumber.c: 138 - 175

## Overview
This static function transfers all relation files for a single database by processing file mappings and handling special cases like visibility map frozen bit requirements during PostgreSQL cluster upgrades.

## Definition
```c
static void transfer_single_new_db(FileNameMap *maps, int size, char *old_tablespace)
```

## Detailed Description
The `transfer_single_new_db` function processes an array of file mappings for a single database, transferring each relation file from the old cluster to the new cluster. It handles the transfer of primary relation files as well as auxiliary files like Free Space Map (_fsm) and Visibility Map (_vm) files. The function includes logic to determine whether visibility map files need to be rewritten when upgrading from older PostgreSQL versions that lack the frozen bit feature in visibility maps.

The function filters mappings based on the old_tablespace parameter, allowing it to process only files from a specific tablespace when running in parallel mode. For each qualifying mapping, it transfers the primary file and any associated auxiliary files.

## Parameters / Member Variables
- `maps`: Array of FileNameMap structures containing file mapping information for the database
- `size`: Number of mappings in the maps array
- `old_tablespace`: Path to the specific old tablespace to process (NULL means process all tablespaces)

## Dependencies
- Functions called/Symbols referenced:
  - [transfer_relfile](transfer_relfile.md)
  - VISIBILITY_MAP_FROZEN_BIT_CAT_VER
  - FileNameMap
- Called from (representative examples):
  - [transfer_all_new_dbs](transfer_all_new_dbs.md)

## Notes and Other Information
- The function determines if visibility maps need special handling by comparing catalog versions between old and new clusters
- It transfers three types of files for each relation: the primary file (""), the free space map ("_fsm"), and the visibility map ("_vm")
- The vm_must_add_frozenbit flag is passed to transfer_relfile to handle visibility map rewriting when upgrading from older versions
- Tablespace filtering allows this function to be used in parallel processing scenarios where different processes handle different tablespaces
- The function is marked static, indicating it is only used within the same source file