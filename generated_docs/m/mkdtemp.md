# mkdtemp

## Location
[src/port/mkdtemp.c:286-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/mkdtemp.c#L286-L293)

## Overview
Creates a unique temporary directory with mode 0700 (read/write/execute for owner only) by replacing trailing 'X' characters in a template path with randomly generated characters.

## Definition

```c
char *
mkdtemp(char *path)
```
## Detailed Description
The  function is a portable implementation for creating temporary directories on platforms that lack the standard POSIX  function. This implementation is derived from NetBSD's code and provides compatibility for PostgreSQL across different operating systems.

The function takes a path template containing trailing 'X' characters and replaces them with a combination of process ID digits and incrementing alphabetic characters to generate a unique directory name. It then attempts to create the directory with restricted permissions (mode 0700) to ensure security.

The function uses the internal  helper function with parameters indicating it should create a directory (not a file) and should not return a file descriptor. If the directory creation is successful, it returns the modified path; otherwise, it returns NULL.

This implementation includes safeguards against denial-of-service attacks by limiting the number of attempts to create unique names and validating that parent directories exist before attempting to create the temporary directory.

## Parameters / Member Variables
- : A null-terminated string containing the template path for the temporary directory. Must end with one or more 'X' characters that will be replaced with unique identifiers. The string is modified in-place if successful.

## Dependencies
- Functions called/Symbols referenced:
  -  (assertion macro for parameter validation)
  -  (internal helper function for generating unique temporary names)
- Called from (representative examples):
  -  (in src/test/regress/pg_regress.c:505)

## Notes and Other Information
- This function is only compiled when the system lacks a native  implementation (controlled by  preprocessor macro)
- The created directory has restrictive permissions (0700) for security, allowing access only to the owner
- The function modifies the input path in-place, so callers should provide a writable string
- If the function fails,  is set appropriately by the underlying system calls
- The implementation is thread-safe through the use of static variables with proper synchronization in the  helper function
- Part of PostgreSQL's portability layer located in  to provide consistent functionality across different platforms