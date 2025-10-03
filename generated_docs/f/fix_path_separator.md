# fix_path_separator

## Location
[src/bin/pg_upgrade/check.c:538-558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L538-L558)

## Overview
Normalizes file path separators for cross-platform compatibility, converting forward slashes to backslashes on Windows systems while leaving paths unchanged on other platforms.

## Definition

```c
static char *
fix_path_separator(char *path)
```
## Detailed Description
This utility function provides platform-specific path separator normalization to ensure compatibility with Windows builtin commands like RMDIR and DEL. On Windows systems, it creates a copy of the input path with all forward slash characters ('/') converted to backslash characters ('\'). On non-Windows platforms, it simply returns the original path pointer unchanged, avoiding unnecessary string duplication.

The function is essential for pg_upgrade operations that need to generate shell scripts or commands that work correctly across different operating systems, particularly when dealing with file system operations.

## Parameters / Member Variables
- `*path`: Input file path string that may contain forward slash separators
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (Windows only)
- Called from (representative examples):
  - [create_script_for_old_cluster_deletion](../c/create_script_for_old_cluster_deletion.md)

## Notes and Other Information
- Windows-specific behavior: Creates a new string copy with converted separators and returns it
- Non-Windows behavior: Returns the original path pointer without modification
- The caller is responsible for freeing the returned string on Windows systems
- Used specifically for generating commands compatible with Windows builtin shell commands
- Compilation conditional using WIN32 preprocessor directive