# archputs

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1629 - 1635

## Overview
A convenience function that outputs a null-terminated string to a PostgreSQL archive during dump operations, mimicking the behavior of standard IO functions.

## Definition
```c
void archputs(const char *s, Archive *AH)
```

## Detailed Description
The `archputs` function provides a simple interface for writing string data to a PostgreSQL archive during database dump operations. It serves as a convenience wrapper around the lower-level `WriteData` function, automatically calculating the string length and handling the data output. This function is designed to mirror the behavior of standard C library functions like `puts()`, but writes to the archive output stream instead of stdout.

The function is part of pg_dump's archiver module, which manages the output formatting and data streaming during database dumps. It ensures that string data is properly written to the archive with correct length calculation.

## Parameters / Member Variables
- `s`: A null-terminated string to be written to the archive
- `AH`: Pointer to the Archive structure containing the output context and formatting information

## Dependencies
- Functions called/Symbols referenced:
  - [WriteData](../W/WriteData.md)
  - strlen (standard C library function)
- Called from (representative examples):
  - [dumpTableData_insert](../d/dumpTableData_insert.md)

## Notes and Other Information
- This function is specifically designed for dump mode operations
- It automatically handles string length calculation using `strlen()`
- The function provides a clean abstraction over the more complex `WriteData` interface
- Part of the convenience functions suite that includes `archprintf` for formatted output
- Located in `src/bin/pg_dump/pg_backup_archiver.c:1629-1635`