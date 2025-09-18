# pg_read_file_all_missing

## Location
src/backend/utils/adt/genfile.c: 333 - 347

## Overview
Wrapper function for the SQL function pg_read_file() that reads an entire text file from beginning to end with an option to handle missing files gracefully.

## Definition
```c
Datum pg_read_file_all_missing(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL built-in function wrapper that implements the two-parameter variant of pg_read_file() with missing file handling. It extracts the filename and a boolean flag indicating whether missing files should be handled gracefully from the function arguments. The function reads the entire file contents by calling pg_read_file_common() with a seek offset of 0 and bytes_to_read of -1 (indicating read all). This wrapper exists to ensure consistency in argument count among PostgreSQL built-in functions that share the same implementing C function.

The function reads the complete contents of a text file and returns it as a PostgreSQL text datum. If the file reading operation fails and the missing_ok flag is set appropriately, it may return NULL without raising an error.

## Parameters / Member Variables
- `filename_t`: Text argument containing the path to the file to be read in its entirety
- `missing_ok`: Boolean flag indicating whether missing files should be handled gracefully (true) or raise an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - PG_GETARG_BOOL (macro for extracting boolean argument)
  - pg_read_file_common (core file reading implementation)
  - PG_RETURN_NULL (macro for returning NULL)
  - PG_RETURN_TEXT_P (macro for returning text datum)
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- This is a wrapper function created specifically to satisfy PostgreSQL's built-in function argument consistency requirements
- The function calls pg_read_file_common with parameters (filename_t, 0, -1, true, missing_ok), where:
  - 0 = start from beginning of file
  - -1 = read until end of file
  - true = read entire file contents
  - missing_ok = caller-specified behavior for missing files
- Located in src/backend/utils/adt/genfile.c:333-347
- Part of PostgreSQL's file reading functionality accessible via SQL
- Combines the simplicity of reading complete file contents with flexible error handling for missing files