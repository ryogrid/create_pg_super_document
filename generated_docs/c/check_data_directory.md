# check_data_directory

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 377 - 408

## Overview
Performs preliminary validation to verify that a given directory is a PostgreSQL cluster data directory by checking for its existence and the presence of the PG_VERSION file.

## Definition


## Detailed Description
The  function performs basic validation checks to determine if a specified directory appears to be a valid PostgreSQL cluster data directory. The function performs two main checks: first, it verifies that the directory exists and is accessible, and second, it checks for the presence of the PG_VERSION file, which is a key indicator of a PostgreSQL data directory.

As noted in the code comments, these are preliminary checks and are not exhaustive. The function is designed to catch obvious errors early in the process, but more thorough validation occurs in subsequent steps of the pg_createsubscriber workflow. If the directory is not actually a proper clone from the publisher, it will fail in later processing steps.

## Parameters
- : The path to the directory that should be checked as a PostgreSQL cluster data directory

## Dependencies
- Functions called/Symbols referenced:
  -  - Logs informational message about the directory being checked
  -  - System call to check file/directory existence and properties
  -  - Safe string formatting to construct the PG_VERSION file path
  -  - Terminates the program with a fatal error message
- Called from:
  -  structure initialization
  -  function for directory validation

## Notes and Other Information
- The function is marked as , indicating it's only used within the pg_createsubscriber.c file
- The function performs only preliminary checks - more comprehensive validation occurs later in the process
- Uses standard filesystem operations () to check directory and file existence
- Will terminate the entire process with  if validation fails
- The PG_VERSION file check is the primary indicator used to identify a PostgreSQL data directory
- Error handling distinguishes between directory not existing (ENOENT) and other access issues
- Designed specifically for validating directories that should be clones from a publisher database