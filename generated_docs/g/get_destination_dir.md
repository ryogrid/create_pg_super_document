# get_destination_dir

## Location
[src/bin/pg_basebackup/pg_receivewal.c:235-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_receivewal.c#L235-L251)

## Overview
A utility function that opens and validates the destination directory for WAL file storage, providing error handling for directory access issues.

## Definition

```c
struct dirent *dirent;
```
## Detailed Description
The get_destination_dir function serves as a safe wrapper around the standard opendir() system call, specifically designed for opening the destination directory where WAL files will be stored. It includes built-in validation through an assertion to ensure the destination folder path is not NULL, and provides comprehensive error handling with descriptive error messages if the directory cannot be opened. The function is essential for pg_receivewal's file management operations, ensuring that the target directory is accessible before attempting to write WAL files to it.

## Parameters / Member Variables
- : Path string to the destination directory that should be opened

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro for debugging)
  - [opendir](../o/opendir.md) (POSIX system call for opening directories)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
  - [DIR](../D/DIR.md) (POSIX directory stream type)
- Called from (representative examples):
  - [FindStreamingStart](../F/FindStreamingStart.md) (in pg_receivewal.c)
  - [main](../m/main.md) (in pg_receivewal.c)

## Notes and Other Information
- This is a static function with file-local scope within pg_receivewal.c
- Provides defensive programming through assertion checking for NULL input
- Uses pg_fatal for error reporting, which terminates the program with an appropriate error message
- Returns a DIR pointer that can be used for subsequent directory operations
- The '%m' format specifier in the error message automatically includes system error details
- Essential for ensuring directory accessibility before WAL file operations begin
- Part of pg_receivewal's initialization and setup process