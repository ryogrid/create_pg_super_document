# make_native_path

## Location
src/port/path.c: 235 - 256

## Overview
Converts forward slash ('/') characters to backslash ('\\') characters in a file path string on Windows platforms to create Windows-native path format.

## Definition
```c
void make_native_path(char *filename)
```

## Detailed Description
This function performs in-place conversion of forward slashes to backslashes in path strings, but only on Windows platforms (when WIN32 is defined). On non-Windows platforms, the function is a no-op. This conversion is the reverse of the debackslash_path function and is specifically needed to handle Windows CMD.EXE internal commands like COPY that require native backslash path separators.

The function is designed to address a specific Windows limitation where CMD.EXE's internal COPY command handles forward slashes differently depending on whether they appear in the first or second argument, and whether the paths are quoted. Unlike debackslash_path, this function doesn't need to worry about multi-byte character encodings because the forward slash character ('/') never appears as part of a multi-byte character in any supported encoding.

## Parameters / Member Variables
- `filename`: The null-terminated string containing the path to be modified in-place to use Windows-native path separators

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only standard C operations)
- Called from (representative examples):
  - [shell_archive_file](../s/shell_archive_file.md)
  - [main](main.md) (in initdb)
  - [pgwin32_CommandLine](../p/pgwin32_CommandLine.md)
  - BuildRestoreCommand

## Notes and Other Information
- This function only operates on Windows platforms (WIN32 build)
- On non-Windows platforms, this function is effectively a no-op
- The function modifies the input path string in-place
- Specifically designed to work around CMD.EXE COPY command limitations with forward slashes
- Unlike debackslash_path, no special encoding handling is needed since '/' cannot be part of multi-byte characters
- This function is part of PostgreSQL's cross-platform compatibility utilities