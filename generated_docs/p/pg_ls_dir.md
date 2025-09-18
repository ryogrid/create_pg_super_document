# pg_ls_dir

## Location
src/backend/utils/adt/genfile.c: 498 - 557

## Overview
Lists the contents of a directory and returns only the filenames as a set-returning function, with optional parameters to handle missing directories and include dot directories.

## Definition


## Detailed Description
The  function is a PostgreSQL system function that provides directory listing functionality accessible from SQL. It reads the contents of a specified directory and returns the filenames as a result set. The function supports up to 3 arguments: the directory path (required), a missing_ok flag (optional), and an include_dot_dirs flag (optional).

The function uses PostgreSQL's set-returning function (SRF) infrastructure to return multiple rows, with each row containing a single filename from the directory. It performs proper error handling for missing directories and can optionally filter out dot directories (. and ..).

## Parameters / Member Variables
- : Directory path to list (converted and validated)
- : missing_ok - if true, returns empty result instead of error when directory doesn't exist
- : include_dot_dirs - if true, includes "." and ".." entries in the results

## Dependencies
- Functions called/Symbols referenced:
  - convert_and_check_filename (validates and converts directory path)
  - InitMaterializedSRF (initializes set-returning function infrastructure)
  - AllocateDir (opens directory for reading)
  - ReadDir (reads directory entries)
  - tuplestore_putvalues (adds result rows to output)
  - FreeDir (closes directory handle)
  - ReturnSetInfo, DIR, dirent (data structures)
  - MAT_SRF_USE_EXPECTED_DESC (SRF configuration constant)
- Called from (representative examples):
  - pg_ls_dir_1arg (wrapper function with fewer parameters)

## Notes and Other Information
- The function validates directory paths through convert_and_check_filename to prevent directory traversal attacks
- Uses PostgreSQL's materialized SRF pattern for efficient result set generation
- Proper resource management with AllocateDir/FreeDir pairing
- Error handling respects the missing_ok parameter to provide graceful degradation
- Dot directory filtering is performed at the application level rather than filesystem level
- Returns empty result set (not NULL) when directory is missing and missing_ok is true