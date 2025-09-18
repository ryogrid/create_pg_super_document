# join_path_components

## Location
[src/port/path.c:285-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L285-L309)

## Overview
Joins two path components together with a forward slash separator, handling edge cases for empty components and ensuring proper path formation.

## Definition
```c
void join_path_components(char *ret_path, const char *head, const char *tail)
```

## Detailed Description
This function combines two path components (head and tail) into a single path string, automatically inserting a forward slash separator when appropriate. The function handles several edge cases intelligently: it omits the slash separator if either component is empty, and it avoids adding a slash if the head component appears to be just a drive specification (determined using skip_drive()).

The function is designed to be safe for in-place operations where the output buffer can be the same as the head input, but explicitly prohibits the output buffer from being the same as the tail input. The resulting path is not canonicalized - that step is left for canonicalize_path() to handle later.

## Parameters / Member Variables
- `ret_path`: Output buffer where the joined path will be stored (must be of size MAXPGPATH)
- `head`: The first path component (directory or drive portion)
- `tail`: The second path component (filename or subdirectory portion)

## Dependencies
- Functions called/Symbols referenced:
  - strlcpy
  - [skip_drive](../s/skip_drive.md)
  - snprintf, strlen (standard C library functions)
- Called from (representative examples):
  - [AbsoluteConfigLocation](../A/AbsoluteConfigLocation.md)
  - [GetConfFilesInDir](../G/GetConfFilesInDir.md)
  - [main](../m/main.md) (in initdb)
  - [process_file](../p/process_file.md)
  - find_my_exec
  - [make_relative_path](../m/make_relative_path.md)

## Notes and Other Information
- The output buffer (ret_path) must be at least MAXPGPATH bytes in size
- ret_path can be the same as head (in-place operation), but must not be the same as tail
- Uses forward slashes as path separators regardless of platform
- Does not perform path canonicalization - that is left for canonicalize_path()
- Handles drive letters on Windows by using skip_drive() to determine if head is just a drive
- Previously attempted to handle "." and ".." components but now leaves that to canonicalization
- Part of PostgreSQL's cross-platform path manipulation utilities