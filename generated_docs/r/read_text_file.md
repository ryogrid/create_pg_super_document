# read_text_file

## Location
[src/backend/utils/adt/genfile.c:211-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L211-L239)

## Overview
Reads a file section as text data, similar to read_binary_file but with database encoding validation to ensure the content is valid text.

## Definition
static text *read_text_file(const char *filename, int64 seek_offset, int64 bytes_to_read, bool missing_ok)

## Detailed Description
This function serves as a text-aware wrapper around read_binary_file, adding crucial encoding validation to ensure that the file contents are valid in the database encoding. It first reads the file content as binary data, then verifies that the bytes form valid multibyte characters according to the current database encoding settings. This validation prevents invalid or corrupted text data from being returned to higher-level functions and helps maintain data integrity. The function maintains the same interface and behavior as read_binary_file but returns a text datum instead of bytea.

## Parameters / Member Variables
- `filename`: Path to the file to be read
- `seek_offset`: Position in file to start reading from (positive from start, negative from end)  
- `bytes_to_read`: Number of bytes to read (-1 means read to end of file)
- `missing_ok`: If true, return NULL for non-existent files instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - [read_binary_file](read_binary_file.md): Core binary file reading functionality
  - [pg_verifymbstr](../p/pg_verifymbstr.md): Validates multibyte string encoding
  - VARDATA: Access variable-length data content
  - VARSIZE: Get variable-length data size
- Called from (representative examples):
  - [pg_read_file_common](../p/pg_read_file_common.md): Higher-level text file reading interface

## Notes and Other Information
- Inherits all the file access capabilities and error handling from read_binary_file
- Adds essential encoding validation for text data integrity
- The encoding validation uses pg_verifymbstr with strict checking (false parameter)
- Returns text datum format compatible with PostgreSQL text handling
- Null return indicates either missing file (when missing_ok is true) or encoding validation failure
- The function safely casts validated bytea to text after verification

## Simplified Source

```c
static text *
read_text_file(const char *filename, int64 seek_offset, int64 bytes_to_read,
               bool missing_ok)
{
    bytea *buf;

    // Read file content as binary data
    buf = read_binary_file(filename, seek_offset, bytes_to_read, missing_ok);

    if (buf != NULL) {
        // Validate that content is valid in database encoding
        pg_verifymbstr(VARDATA(buf), VARSIZE(buf) - VARHDRSZ, false);

        // Safe to cast to text after validation
        return (text *) buf;
    }

    return NULL;
}
```