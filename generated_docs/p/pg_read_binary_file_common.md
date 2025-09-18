# pg_read_binary_file_common

## Location
[src/backend/utils/adt/genfile.c:260-284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L260-L284)

## Overview
High-level interface for reading binary files with integrated security checking and parameter validation, parallel to pg_read_file_common but for binary data.

## Definition
static bytea *pg_read_binary_file_common(text *filename_t, int64 seek_offset, int64 bytes_to_read, bool read_to_eof, bool missing_ok)

## Detailed Description
This function provides a comprehensive binary file reading interface that mirrors the functionality of pg_read_file_common but returns binary data instead of text. It combines security validation through convert_and_check_filename with the core binary reading capabilities of read_binary_file. The function supports the same flexible parameter model as its text counterpart, including precise byte-count reading and read-to-EOF operations. It maintains consistent parameter validation and error handling while preserving the raw binary nature of the file content without any encoding validation or transformation.

## Parameters / Member Variables
- `filename_t`: PostgreSQL text datum containing the filename to read
- `seek_offset`: Position in file to start reading from (positive from start, negative from end)
- `bytes_to_read`: Number of bytes to read (-1 when read_to_eof is true)  
- `read_to_eof`: When true, read from current position to end of file
- `missing_ok`: If true, return NULL for non-existent files instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - [convert_and_check_filename](../c/convert_and_check_filename.md): Security validation and filename conversion
  - [read_binary_file](../r/read_binary_file.md): Core binary file reading functionality
  - ereport: Error reporting for invalid parameters
- Called from (representative examples):
  - PostgreSQL SQL functions that need to read binary files
  - System administration utilities requiring binary file access

## Notes and Other Information
- Parameters are interpreted identically to pg_read_file_common for consistency
- When read_to_eof is true, bytes_to_read must be exactly -1 (enforced by assertion)
- Negative bytes_to_read values are only allowed when read_to_eof is true
- Returns bytea format suitable for PostgreSQL binary data handling
- No encoding validation is performed, preserving exact binary content
- Uses the same security model as the text reading functions through convert_and_check_filename
- Provides consistent error handling and parameter validation across the file reading API family