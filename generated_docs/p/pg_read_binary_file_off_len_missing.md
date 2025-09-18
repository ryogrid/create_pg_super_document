# pg_read_binary_file_off_len_missing

## Location
src/backend/utils/adt/genfile.c: 364 - 380

## Overview
A PostgreSQL system function that reads a binary file from a specified offset for a specified length, with an option to handle missing files gracefully.

## Definition


## Detailed Description
This function is a PostgreSQL built-in function that provides controlled binary file reading capabilities with offset positioning, length limiting, and missing file tolerance. It serves as a wrapper around the common binary file reading functionality () with specific parameters for handling file access scenarios where the file may not exist. The function reads binary data from a file starting at a specific byte offset and reads up to a specified number of bytes, returning the data as a bytea (binary data) type or NULL if the file is missing and the missing_ok flag is set.

## Parameters / Member Variables
- : Text parameter containing the path to the file to be read
- : 64-bit integer specifying the byte offset from which to start reading
- : 64-bit integer specifying the maximum number of bytes to read
- : Boolean flag indicating whether to return NULL instead of raising an error if the file doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - PG_GETARG_INT64 (macro for extracting 64-bit integer argument)
  - PG_GETARG_BOOL (macro for extracting boolean argument)
  - [pg_read_binary_file_common](pg_read_binary_file_common.md) (core file reading implementation)
  - PG_RETURN_BYTEA_P (macro for returning bytea data)
  - PG_RETURN_NULL (macro for returning NULL)
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- Located in src/backend/utils/adt/genfile.c:364-380
- This function provides the implementation for a SQL-callable function that allows controlled binary file access
- The missing_ok parameter allows for non-fatal handling of missing files, making it suitable for optional file operations
- Returns bytea data type on success, NULL when file is missing and missing_ok is true
- Uses the common file reading infrastructure to ensure consistent security and error handling