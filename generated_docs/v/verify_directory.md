# verify_directory

## Location
[src/bin/pg_waldump/pg_waldump.c:113-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L113-L127)

## Overview
A utility function that checks whether a given directory exists and can be opened for reading.

## Definition
```c
static bool verify_directory(const char *directory)
```

## Detailed Description
The verify_directory function performs a simple but essential validation check to determine if a specified directory path exists and is accessible. It attempts to open the directory using opendir() and immediately closes it if successful. The function is designed to preserve the errno value set by opendir() in case of failure, allowing the caller to provide more accurate error reporting. This function is commonly used in pg_waldump to validate directory paths before attempting to process WAL files within them.

## Parameters / Member Variables
- `directory`: A null-terminated string containing the path to the directory to be verified

## Dependencies
- Functions called/Symbols referenced:
  - [DIR](../D/DIR.md) (directory stream type)
  - [opendir](../o/opendir.md) (system function to open a directory stream)
  - [closedir](../c/closedir.md) (system function to close a directory stream)
- Called from (representative examples):
  - [main](../m/main.md) (called in pg_waldump.c:1102)
  - [main](../m/main.md) (called in pg_waldump.c:1126)

## Notes and Other Information
- The function preserves errno on failure to enable accurate error reporting by the caller
- Only checks if the directory can be opened, not whether it contains valid WAL files
- Returns true if the directory exists and is accessible, false otherwise
- Used for validating both WAL directory paths and output directory paths in pg_waldump
- The function performs minimal validation - it does not check directory permissions beyond basic read access