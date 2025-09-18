# db_dir_size

## Location
src/backend/utils/adt/dbsize.c: 74 - 117

## Overview
A static utility function that calculates the physical size of a directory by recursively summing the sizes of all files within it, returning 0 if the directory doesn't exist.

## Definition


## Detailed Description
The  function performs a directory traversal to calculate the total size of all files contained within a specified directory path. It opens the directory using , iterates through all directory entries using , and accumulates the file sizes by calling  on each file. The function handles errors gracefully - if the directory doesn't exist, it returns 0, and if individual files cannot be accessed due to ENOENT (file not found), it skips them. For other stat errors, it reports an error. The function also includes interruption checking to allow query cancellation during long operations.

## Parameters / Member Variables
- : The filesystem path to the directory whose size should be calculated

## Dependencies
- Functions called/Symbols referenced:
  - : Opens a directory for reading
  - : Reads directory entries 
  - : Closes and frees directory resources
  - : Gets file status information including size
  - : Allows query cancellation
  - : Reports errors
  - : Provides error codes for file access failures
- Called from (representative examples):
  - : Uses this to calculate sizes of database directories
  - : Uses this to calculate sizes of tablespace directories

## Notes and Other Information
- This is a static function, meaning it's only accessible within the dbsize.c file
- The function skips "." and ".." directory entries to avoid counting parent directories
- File size accumulation uses int64 to handle large directory sizes
- The function is designed to be resilient to files that disappear during traversal (ENOENT handling)
- Error handling follows PostgreSQL conventions using ereport for non-recoverable errors