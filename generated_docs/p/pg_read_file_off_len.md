# pg_read_file_off_len

## Location
[src/backend/utils/adt/genfile.c:285-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L285-L300)

## Overview
Wrapper function for the SQL function pg_read_file() that reads a specified portion of a text file starting from a given offset with a specified length.

## Definition
```c
Datum pg_read_file_off_len(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL built-in function wrapper that implements the three-parameter variant of pg_read_file(). It extracts the filename, seek offset, and number of bytes to read from the function arguments, then delegates the actual file reading operation to pg_read_file_common(). The function is designed to pass sanity checks that ensure all built-in functions sharing the same implementing C function take the same number of arguments.

The function reads text files and returns the content as a PostgreSQL text datum. If the file reading operation fails, it returns NULL.

## Parameters / Member Variables
- `filename_t`: Text argument containing the path to the file to be read
- `seek_offset`: 64-bit integer specifying the byte offset from the beginning of the file where reading should start
- `bytes_to_read`: 64-bit integer specifying the maximum number of bytes to read from the file

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - PG_GETARG_INT64 (macro for extracting 64-bit integer arguments)
  - [pg_read_file_common](pg_read_file_common.md) (core file reading implementation)
  - PG_RETURN_NULL (macro for returning NULL)
  - PG_RETURN_TEXT_P (macro for returning text datum)
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- This is a wrapper function created specifically to satisfy PostgreSQL's built-in function argument consistency requirements
- The function uses the fourth and fifth parameters of pg_read_file_common as false, indicating it reads text files (not binary) and does not allow missing files
- Located in src/backend/utils/adt/genfile.c:285-300
- Part of PostgreSQL's file reading functionality accessible via SQL