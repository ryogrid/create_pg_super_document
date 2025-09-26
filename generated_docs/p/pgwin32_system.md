# pgwin32_system

## Location
[src/port/system.c:53-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/system.c#L53-L85)

## Overview
A Windows-specific wrapper function that executes system commands by adding extra quotes around the command string to handle arguments with spaces safely.

## Definition

```c
int
pgwin32_system(const char *command)
```
## Detailed Description
The  function is a Windows-specific implementation that wraps the standard C library  function. Its primary purpose is to handle command strings that may contain spaces or special characters by automatically enclosing the entire command in double quotes. This prevents issues with argument parsing on Windows systems where spaces in paths or arguments can cause command execution to fail.

The function creates a dynamically allocated buffer that is 2 characters larger than the input command to accommodate the surrounding quotes, then calls the standard  function with this quoted version. After execution, it properly cleans up the allocated memory while preserving any errno value that may have been set during execution.

## Parameters / Member Variables
- : A null-terminated string containing the system command to execute

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - system
- Called from (representative examples):
  - [pclose](pclose.md) (referenced in src/include/port.h)

## Notes and Other Information
- This function is Windows-specific and part of PostgreSQL's portability layer
- Memory allocation failure returns -1 with errno set to ENOMEM
- The function preserves the errno value from the system() call
- The extra quotes help handle Windows paths and arguments that contain spaces
- Located in src/port/system.c as part of the platform abstraction layer