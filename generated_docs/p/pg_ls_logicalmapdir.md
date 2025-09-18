# pg_ls_logicalmapdir

## Location
src/backend/utils/adt/genfile.c: 705 - 714

## Overview
Lists the files in the PostgreSQL logical replication mappings directory (pg_logical/mappings).

## Definition


## Detailed Description
This function is a SQL-callable PostgreSQL function that provides access to the contents of the pg_logical/mappings directory. It serves as a wrapper around the generic pg_ls_dir_files function, specifically targeting the directory where logical replication mapping files are stored. The function returns detailed information about regular files in this directory, including file names, sizes, and modification times.

The pg_logical/mappings directory contains files that are part of PostgreSQL's logical replication infrastructure, storing mapping information used during logical decoding operations.

## Parameters / Member Variables
- : Function call information structure (standard PostgreSQL function parameter)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ls_dir_files](pg_ls_dir_files.md)
- Called from (representative examples):
  - Available as SQL function but no direct callers found in codebase

## Notes and Other Information
- This function is part of PostgreSQL's file system access functions for administrative purposes
- It specifically targets the logical replication mappings directory
- Uses the generic pg_ls_dir_files function with hardcoded path "pg_logical/mappings" and false parameter (meaning it lists regular files only)
- Located in src/backend/utils/adt/genfile.c:705-714
- The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS