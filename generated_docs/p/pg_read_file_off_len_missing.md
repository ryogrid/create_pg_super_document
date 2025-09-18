# pg_read_file_off_len_missing

## Location
src/backend/utils/adt/genfile.c: 301 - 318

## Overview
Wrapper function for the SQL function pg_read_file() that reads a specified portion of a text file starting from a given offset with a specified length, with an option to handle missing files gracefully.

## Definition
```c
Datum pg_read_file_off_len_missing(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL built-in function wrapper that implements the four-parameter variant of pg_read_file() with missing file handling. It extracts the filename, seek offset, number of bytes to read, and a boolean flag indicating whether missing files should be handled gracefully from the function arguments. The actual file reading operation is delegated to pg_read_file_common(). This wrapper exists to ensure consistency in argument count among PostgreSQL built-in functions that share the same implementing C function.

The function reads text files and returns the content as a PostgreSQL text datum. If the file reading operation fails and the missing_ok flag is set appropriately, it may return NULL without raising an error.

## Parameters / Member Variables
- `filename_t`: Text argument containing the path to the file to be read
- `seek_offset`: 64-bit integer specifying the byte offset from the beginning of the file where reading should start
- `bytes_to_read`: 64-bit integer specifying the maximum number of bytes to read from the file
- `missing_ok`: Boolean flag indicating whether missing files should be handled gracefully (true) or raise an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - PG_GETARG_INT64 (macro for extracting 64-bit integer arguments)
  - PG_GETARG_BOOL (macro for extracting boolean argument)
  - pg_read_file_common (core file reading implementation)
  - PG_RETURN_NULL (macro for returning NULL)
  - PG_RETURN_TEXT_P (macro for returning text datum)
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- This is a wrapper function created specifically to satisfy PostgreSQL's built-in function argument consistency requirements
- The function uses the fourth parameter of pg_read_file_common as false (indicating text file reading, not binary) and the fifth parameter as the missing_ok value passed by the caller
- Located in src/backend/utils/adt/genfile.c:301-318
- Part of PostgreSQL's file reading functionality accessible via SQL
- The missing_ok parameter provides more flexible error handling compared to the three-parameter variant