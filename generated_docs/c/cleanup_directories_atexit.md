# cleanup_directories_atexit

## Location
src/bin/pg_basebackup/pg_basebackup.c: 236 - 282

## Overview
A cleanup function registered with atexit() that removes data and WAL directories created during initdb if the operation fails.

## Definition


## Detailed Description
This function serves as an exit handler for the initdb utility, responsible for cleaning up any directories and files created during database initialization if the process fails or is interrupted. The function checks the global  flag and performs cleanup operations based on various state variables that track what directories were created or found during initialization.

The cleanup behavior depends on:
- Whether initialization was successful ( flag)
- Whether cleanup is disabled ( flag)  
- Whether new directories were created vs. existing ones were found
- The specific directories involved (data directory and WAL directory)

When cleanup is performed, it uses different strategies:
- For newly created directories: removes the entire directory tree
- For existing directories that had content added: removes only the contents

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- : Boolean indicating if initdb completed successfully
- : Boolean flag to disable cleanup (--no-clean option)
- : Boolean indicating if a new data directory was created
- : Boolean indicating if an existing data directory was found
- : Boolean indicating if a new WAL directory was created
- : Boolean indicating if an existing WAL directory was found
- : Path to the data directory
- : Path to the WAL directory

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info (for logging cleanup actions)
  - rmtree (for recursive directory removal)
  - pg_log_error (for error logging when removal fails)
- Called from (representative examples):
  - main (in initdb.c via atexit registration)
  - main (in pg_basebackup.c via atexit registration)
  - main (in pg_combinebackup.c via atexit registration)

## Notes and Other Information
- This is a static function, meaning it has internal linkage within initdb.c
- The function is typically registered with atexit() early in the main() function
- It provides a safety mechanism to prevent leaving partially initialized database clusters
- The --no-clean command line option can disable the cleanup behavior for debugging purposes
- Different cleanup strategies are used for newly created vs. pre-existing directories to avoid data loss