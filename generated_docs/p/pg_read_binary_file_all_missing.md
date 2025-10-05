# pg_read_binary_file_all_missing

## Location
[src/backend/utils/adt/genfile.c:395-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L395-L412)

## Overview
A PostgreSQL system function that reads an entire binary file from the beginning with an option to handle missing files gracefully.

## Definition

```c
struct stat fst;
```
## Detailed Description
This function combines the functionality of complete file reading with optional missing file tolerance. It serves as a wrapper around the common binary file reading functionality () with parameters configured to read the entire file from the beginning (offset 0) with no size limit (-1 for bytes_to_read). The key feature is the  parameter that allows the function to return NULL instead of raising an error when the specified file doesn't exist. This makes it suitable for optional file operations where the absence of a file is not necessarily an error condition.

## Parameters / Member Variables
- : Text parameter containing the path to the file to be read completely
- : Boolean flag indicating whether to return NULL instead of raising an error if the file doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - PG_GETARG_BOOL (macro for extracting boolean argument)
  - [pg_read_binary_file_common](pg_read_binary_file_common.md) (core file reading implementation with parameters: offset=0, length=-1, enforce_size=true, missing_ok parameter)
  - PG_RETURN_BYTEA_P (macro for returning bytea data)
  - PG_RETURN_NULL (macro for returning NULL)
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- Located in src/backend/utils/adt/genfile.c:395-412
- This function provides the implementation for a SQL-callable function for complete file reading with missing file tolerance
- Uses hardcoded parameters: offset=0 (read from beginning), length=-1 (read entire file), enforce_size=true
- The missing_ok parameter is user-controlled, allowing flexible error handling
- Returns the entire file content as bytea data type on success, NULL when file is missing and missing_ok is true
- Provides a balance between complete file access and graceful handling of optional files
- Useful for configuration files, optional binary resources, or conditional file operations

## Simplified Source

```c
Datum pg_read_binary_file_all_missing(PG_FUNCTION_ARGS) {
    // Extract filename and missing_ok flag from function arguments
    text *filename_t = PG_GETARG_TEXT_PP(0);
    bool missing_ok = PG_GETARG_BOOL(1);

    // Read entire binary file: offset=0, length=-1 (all), enforce_size=true, with missing file handling
    text *ret = pg_read_binary_file_common(filename_t, 0, -1, true, missing_ok);

    // Return binary data or NULL if reading failed
    if (!ret)
        PG_RETURN_NULL();

    PG_RETURN_BYTEA_P(ret);
}
```