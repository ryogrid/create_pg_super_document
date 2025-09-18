# pg_current_logfile_1arg

## Location
src/backend/utils/adt/misc.c: 1092 - 1100

## Overview
A wrapper function that provides a single-argument version of pg_current_logfile for PostgreSQL's built-in function system compatibility.

## Definition


## Detailed Description
This function serves as a compatibility wrapper around the main  function. It exists specifically to satisfy PostgreSQL's opr_sanity checks, which require that all built-in functions sharing the same implementing C function must take the same number of arguments. The function simply delegates to the main  function, which reports the current log file used by the log collector by scanning the current_logfiles metadata.

The underlying functionality reads the LOG_METAINFO_DATAFILE to find the current log file path for the specified log format (stderr, csvlog, or jsonlog). When no specific format is provided, it returns the first available log file.

## Parameters / Member Variables
- : Function call information structure containing the single argument (log format parameter)

## Dependencies
- Functions called/Symbols referenced:
  - pg_current_logfile
- Called from (representative examples):
  - SQL function calls: 

## Notes and Other Information
- This wrapper is necessary for PostgreSQL's type system integrity checks
- The actual log file resolution logic is implemented in the main pg_current_logfile function
- Located in src/backend/utils/adt/misc.c:1092-1100
- Used internally by PostgreSQL's function call mechanism when the single-argument version is invoked