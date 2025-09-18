# pg_read_binary_file_all

## Location
src/backend/utils/adt/genfile.c: 381 - 394

## Overview
A PostgreSQL system function that reads an entire binary file from the beginning without size restrictions.

## Definition


## Detailed Description
This function provides a simple interface for reading complete binary files in PostgreSQL. It serves as a wrapper around the common binary file reading functionality () with parameters configured to read the entire file from the beginning (offset 0) with no size limit (-1 for bytes_to_read). The function is designed for scenarios where the entire contents of a binary file need to be loaded into memory as bytea data. It does not handle missing files gracefully and will raise an error if the file doesn't exist.

## Parameters / Member Variables
- : Text parameter containing the path to the file to be read completely

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - [pg_read_binary_file_common](pg_read_binary_file_common.md) (core file reading implementation with parameters: offset=0, length=-1, enforce_size=true, missing_ok=false)
  - PG_RETURN_BYTEA_P (macro for returning bytea data)
  - PG_RETURN_NULL (macro for returning NULL)
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- Located in src/backend/utils/adt/genfile.c:381-394
- This function provides the implementation for a SQL-callable function for complete file reading
- Uses hardcoded parameters: offset=0 (read from beginning), length=-1 (read entire file), enforce_size=true, missing_ok=false (file must exist)
- Returns the entire file content as bytea data type
- Will raise an error if the file doesn't exist or cannot be read
- Suitable for loading complete configuration files, small binary data files, or other scenarios where the entire file content is needed