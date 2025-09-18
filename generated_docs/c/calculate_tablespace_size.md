# calculate_tablespace_size

## Location
src/backend/utils/adt/dbsize.c: 202 - 271

## Overview
A static function that calculates the total physical size of a tablespace, including all databases and objects stored within it, returning -1 if the tablespace directory cannot be found.

## Definition


## Detailed Description
The  function computes the total size of a tablespace by scanning its directory structure and summing the sizes of all files and subdirectories. It first performs access control checking to ensure the user has either pg_read_all_stats role privileges or CREATE privilege on the target tablespace (with an exception for the current database's default tablespace). The function handles three special tablespace cases: DEFAULTTABLESPACE_OID maps to the "base" directory, GLOBALTABLESPACE_OID maps to "global", and custom tablespaces map to "pg_tblspc/oid/version_directory". It then iterates through all entries in the tablespace directory, using  to get file sizes and recursively calling  for subdirectories. The function includes both individual file sizes and directory contents in the total calculation.

## Parameters / Member Variables
- : The OID (Object Identifier) of the tablespace whose size should be calculated

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if user has privileges of pg_read_all_stats role
  - : Checks access control permissions for the tablespace
  - : Reports access control errors
  - : Gets the name of a tablespace from its OID
  - : Opens a directory for reading
  - : Reads directory entries
  - : Closes and frees directory resources
  - : Gets file status information including size and type
  - : Recursively calculates directory sizes
  - : Macro to test if a file is a directory
  - : Allows query cancellation
  - : Reports errors for file access failures
  - : Global variable for current database's tablespace
  - : Role constant for read-all-stats privilege
  - : Access control constant for create privilege
  - : Object type constant for tablespaces
  - , : Special tablespace OID constants
  - : Directory name for tablespace versions
- Called from (representative examples):
  - : Public function that takes a tablespace OID
  - : Public function that takes a tablespace name

## Notes and Other Information
- This is a static function, only accessible within dbsize.c
- Returns -1 specifically when the tablespace directory cannot be found, distinguishing it from a size of 0
- Performs different access control logic than database size functions - allows access to current database's tablespace
- Handles three distinct path patterns for different types of tablespaces (default, global, custom)
- Includes both direct files and subdirectory contents in the total size calculation
- Uses recursive directory traversal via db_dir_size for subdirectories
- The function handles interruption checking during directory scanning for long operations
- Error handling follows PostgreSQL conventions, continuing on ENOENT but reporting other stat failures
- Total size calculation includes everything within the tablespace across all databases that use it