# path_is_prefix_of_path

## Location
[src/port/path.c:636-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L636-L650)

## Overview
Determines whether one path is a proper prefix of another path, including the case where the paths are identical.

## Definition

```c
bool
path_is_prefix_of_path(const char *path1, const char *path2)
```
## Detailed Description
The  function checks if  is a prefix of  in a path-aware manner. This means it not only performs string prefix matching but also ensures the prefix boundary occurs at a proper path component boundary.

The function performs two checks:
1. **String prefix match**: Uses  to verify that the beginning of  matches  exactly
2. **Boundary validation**: Ensures that the match occurs at a path component boundary by checking that the character immediately following the prefix in  is either:
   - A directory separator (using the  macro to handle platform differences)
   - The null terminator (indicating  is exactly equal to )

This prevents false positives where a path like '/tmp' would incorrectly match '/tmp123' - the function correctly requires '/tmp' to be followed by '/' or end-of-string.

## Parameters / Member Variables
- : A null-terminated string representing the potential prefix path to test against
- : A null-terminated string representing the path to check for having  as a prefix

## Dependencies
- Functions called/Symbols referenced:
  -  (platform-aware directory separator macro)
- Called from (representative examples):
  -  (src/backend/commands/tablespace.c:271)
  -  (src/backend/utils/adt/genfile.c:79, 81)
  -  (src/bin/pg_upgrade/check.c:931, 954)
  -  (src/bin/pg_upgrade/option.c:274)

## Notes and Other Information
- This function is designed to be simple but correct, handling path prefix matching in a platform-aware way
- The use of  macro ensures proper handling of directory separators across different operating systems (Unix '/' vs Windows '\')
- Returns true for exact path equality (when  and  are identical)
- Commonly used in security contexts to verify that file paths remain within allowed directory hierarchies
- The function is deliberately simple and focuses on correctness rather than performance for this fundamental path operation
- Used extensively in PostgreSQL's path validation logic, particularly in tablespace and file access controls

## Simplified Source

```c
bool path_is_prefix_of_path(const char *path1, const char *path2) {
    int path1_len = strlen(path1);

    // Check if path1 matches the beginning of path2
    // and that the match ends at a proper path boundary
    if (strncmp(path1, path2, path1_len) == 0 &&
        (IS_DIR_SEP(path2[path1_len]) || path2[path1_len] == '\0')) {
        return true;
    }

    return false;
}
```