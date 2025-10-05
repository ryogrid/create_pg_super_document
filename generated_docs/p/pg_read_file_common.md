# pg_read_file_common

## Location
[src/backend/utils/adt/genfile.c:240-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L240-L259)

## Overview
High-level interface for reading text files with integrated security checking, parameter validation, and flexible reading modes.

## Definition
static text *pg_read_file_common(text *filename_t, int64 seek_offset, int64 bytes_to_read, bool read_to_eof, bool missing_ok)

## Detailed Description
This function provides a comprehensive text file reading interface that combines security validation, parameter checking, and flexible reading operations. It serves as the primary entry point for PostgreSQL's text file reading capabilities, integrating filename security validation through convert_and_check_filename with the core reading functionality of read_text_file. The function supports both precise byte-count reading and read-to-EOF operations, with built-in parameter validation to ensure consistent behavior. It implements PostgreSQL's privilege-based file access model where permissions are handled through the GRANT system rather than superuser checks.

## Parameters / Member Variables
- `filename_t`: PostgreSQL text datum containing the filename to read
- `seek_offset`: Position in file to start reading from (positive from start, negative from end)
- `bytes_to_read`: Number of bytes to read (-1 when read_to_eof is true)
- `read_to_eof`: When true, read from current position to end of file
- `missing_ok`: If true, return NULL for non-existent files instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - [convert_and_check_filename](../c/convert_and_check_filename.md): Security validation and filename conversion
  - [read_text_file](../r/read_text_file.md): Core text file reading with encoding validation
  - ereport: Error reporting for invalid parameters
- Called from (representative examples):
  - Various PostgreSQL SQL functions that need to read text files
  - File administration and maintenance utilities

## Notes and Other Information
- Privileges are handled by PostgreSQL's GRANT system, not by superuser checks within this function
- When read_to_eof is true, bytes_to_read must be exactly -1 (enforced by assertion)
- Negative bytes_to_read values are only allowed when read_to_eof is true
- The function provides clear parameter validation with appropriate error codes (ERRCODE_INVALID_PARAMETER_VALUE)
- Integrates seamlessly with PostgreSQL's role-based security model
- Returns text format suitable for PostgreSQL text data handling

## Simplified Source

```c
static text *
pg_read_file_common(text *filename_t, int64 seek_offset, int64 bytes_to_read,
                    bool read_to_eof, bool missing_ok)
{
    // Validate parameters based on read mode
    if (read_to_eof)
        Assert(bytes_to_read == -1);
    else if (bytes_to_read < 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("requested length cannot be negative")));

    // Validate filename security and read file
    return read_text_file(convert_and_check_filename(filename_t),
                         seek_offset, bytes_to_read, missing_ok);
}
```