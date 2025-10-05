# pg_read_binary_file_off_len

## Location
[src/backend/utils/adt/genfile.c:348-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L348-L363)

## Overview
Wrapper function for the SQL function pg_read_binary_file() that reads a specified portion of a binary file starting from a given offset with a specified length.

## Definition
```c
Datum pg_read_binary_file_off_len(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL built-in function wrapper that implements the three-parameter variant of pg_read_binary_file(). It extracts the filename, seek offset, and number of bytes to read from the function arguments, then delegates the actual file reading operation to pg_read_binary_file_common(). Unlike the text file reading functions, this function is designed for reading binary files and returns the content as a PostgreSQL bytea (byte array) datum.

The function is designed to pass sanity checks that ensure all built-in functions sharing the same implementing C function take the same number of arguments. If the file reading operation fails, it returns NULL.

## Parameters / Member Variables
- `filename_t`: Text argument containing the path to the binary file to be read
- `seek_offset`: 64-bit integer specifying the byte offset from the beginning of the file where reading should start
- `bytes_to_read`: 64-bit integer specifying the maximum number of bytes to read from the file

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - PG_GETARG_INT64 (macro for extracting 64-bit integer arguments)
  - [pg_read_binary_file_common](pg_read_binary_file_common.md) (core binary file reading implementation)
  - PG_RETURN_NULL (macro for returning NULL)
  - PG_RETURN_BYTEA_P (macro for returning bytea datum)
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- This is a wrapper function created specifically to satisfy PostgreSQL's built-in function argument consistency requirements
- The function uses pg_read_binary_file_common with the fourth and fifth parameters as false, indicating it does not read entire file and does not allow missing files
- Located in src/backend/utils/adt/genfile.c:348-363
- Part of PostgreSQL's binary file reading functionality accessible via SQL
- Returns bytea data type instead of text, making it suitable for reading non-text files like images, executables, or other binary data
- Differs from text file reading functions by using pg_read_binary_file_common instead of pg_read_file_common

## Simplified Source

```c
Datum pg_read_binary_file_off_len(PG_FUNCTION_ARGS) {
    // Extract filename, offset, and length from function arguments
    text *filename_t = PG_GETARG_TEXT_PP(0);
    int64 seek_offset = PG_GETARG_INT64(1);
    int64 bytes_to_read = PG_GETARG_INT64(2);

    // Read binary file with specified offset and length: read_all=false, missing_ok=false
    text *ret = pg_read_binary_file_common(filename_t, seek_offset, bytes_to_read, false, false);

    // Return binary data or NULL if reading failed
    if (!ret)
        PG_RETURN_NULL();

    PG_RETURN_BYTEA_P(ret);
}
```