# pg_read_file_all

## Location
[src/backend/utils/adt/genfile.c:319-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L319-L332)

## Overview
Wrapper function for the SQL function pg_read_file() that reads an entire text file from beginning to end.

## Definition
```c
Datum pg_read_file_all(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL built-in function wrapper that implements the single-parameter variant of pg_read_file(). It extracts only the filename from the function arguments and reads the entire file contents by calling pg_read_file_common() with a seek offset of 0 and bytes_to_read of -1 (indicating read all). This wrapper exists to ensure consistency in argument count among PostgreSQL built-in functions that share the same implementing C function.

The function reads the complete contents of a text file and returns it as a PostgreSQL text datum. If the file reading operation fails, it returns NULL.

## Parameters / Member Variables
- `filename_t`: Text argument containing the path to the file to be read in its entirety

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - [pg_read_file_common](pg_read_file_common.md) (core file reading implementation)
  - PG_RETURN_NULL (macro for returning NULL)
  - PG_RETURN_TEXT_P (macro for returning text datum)
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- This is a wrapper function created specifically to satisfy PostgreSQL's built-in function argument consistency requirements
- The function calls pg_read_file_common with parameters (filename_t, 0, -1, true, false), where:
  - 0 = start from beginning of file
  - -1 = read until end of file
  - true = read entire file contents
  - false = do not allow missing files (will raise error if file doesn't exist)
- Located in src/backend/utils/adt/genfile.c:319-332
- Part of PostgreSQL's file reading functionality accessible via SQL
- Most straightforward variant for reading complete file contents