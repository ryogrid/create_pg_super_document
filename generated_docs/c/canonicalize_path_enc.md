# canonicalize_path_enc

## Location
[src/port/path.c:343-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L343-L575)

## Overview
The encoding-aware version of path canonicalization that cleans up and normalizes file system paths while properly handling character encoding considerations.

## Definition

```c
void
canonicalize_path_enc(char *path, int encoding)
```
## Detailed Description
The  function is the core implementation for path canonicalization in PostgreSQL. It performs comprehensive path normalization while being aware of character encoding to safely handle multi-byte characters. The function modifies the path in-place using a sophisticated state machine approach.

The function performs these operations:
1. **Windows-specific processing**: Converts backslashes to forward slashes and removes trailing quotes
2. **Separator normalization**: Removes trailing slashes and eliminates duplicate adjacent separators
3. **Component processing**: Handles '.' and '..' directory references using a state machine that tracks:
   - Absolute vs relative paths
   - Path depth for proper '..' resolution
   - Parent reference tracking for relative paths

The state machine manages four states:
- : Starting state for absolute paths
- : Absolute path with known directory depth
- : Starting state for relative paths  
- : Relative path containing irreducible parent references
- : Relative path with known directory depth

## Parameters / Member Variables
- `*path`: A null-terminated string containing the file system path to be canonicalized. The path is modified in-place.
- `encoding`: Integer specifying the character encoding of the path string (e.g., PG_UTF8, PG_SQL_ASCII) to ensure safe multi-byte character handling.
## Dependencies
- Functions called/Symbols referenced:
  -  (Windows path conversion)
  - 
  -  (Windows drive handling)
  - 
  - 
  - State constants: , , , , 
- Called from (representative examples):
  -  (src/port/path.c:339)
  -  (src/bin/psql/command.c:1134)
  -  (src/bin/psql/command.c:2788)
  -  (src/bin/psql/command.c:4394)
  -  (src/bin/psql/copy.c:283)

## Notes and Other Information
- This is the encoding-aware variant that should be used when dealing with paths that may contain multi-byte characters
- The function implements a sophisticated state machine to correctly handle complex path combinations like '../dir/..' 
- Windows-specific logic handles drive letters and UNC paths appropriately
- The algorithm ensures that the output path is never longer than the input, making in-place modification safe
- Empty paths are preserved, and paths that reduce to nothing are converted to '.'
- Critical for safe path handling in PostgreSQL's multi-encoding environment, particularly in psql and file operations

## Simplified Source

```c
// Simplified version of canonicalize_path_enc
void canonicalize_path_enc(char *path, int encoding) {
    char *p, *to_p;
    char *spath;
    char *parsed;
    char *unparse;
    bool was_sep = false;
    canonicalize_state state;
    int pathdepth = 0;  // counts directory depth

#ifdef WIN32
    // Convert backslashes to forward slashes for Windows
    debackslash_path(path, encoding);

    // Remove trailing quote if present
    p = path + strlen(path);
    if (p > path && *(p - 1) == '"')
        *(p - 1) = '/';
#endif

    // Remove trailing slashes (except leading slash)
    trim_trailing_separator(path);

    // Remove duplicate adjacent separators like "///"
    p = path;
#ifdef WIN32
    if (*p) p++;  // Don't remove leading double-slash on Win32
#endif
    to_p = p;
    for (; *p; p++, to_p++) {
        // Skip multiple consecutive slashes
        while (*p == '/' && was_sep)
            p++;
        if (to_p != p)
            *to_p = *p;
        was_sep = (*p == '/');
    }
    *to_p = '\0';

    // Process "." and ".." components using state machine
    spath = skip_drive(path);
    if (*spath == '\0')
        return;  // empty path

    // Initialize state and parsing pointers
    if (*spath == '/') {
        state = ABSOLUTE_PATH_INIT;
        parsed = unparse = (spath + 1);  // Skip leading slash
    } else {
        state = RELATIVE_PATH_INIT;
        parsed = unparse = spath;
    }

    // Main parsing loop - process each path component
    while (*unparse != '\0') {
        char *unparse_next;
        bool is_double_dot;

        // Extract next directory name
        unparse_next = unparse;
        while (*unparse_next && *unparse_next != '/')
            unparse_next++;
        if (*unparse_next != '\0')
            *unparse_next++ = '\0';

        // Handle "." components (ignore them)
        if (strcmp(unparse, ".") == 0) {
            unparse = unparse_next;
            continue;
        }

        // Check if this is ".." component
        is_double_dot = (strcmp(unparse, "..") == 0);

        // State machine for handling path components
        switch (state) {
            case ABSOLUTE_PATH_INIT:
                if (!is_double_dot) {
                    // Add first directory after root
                    parsed = append_subdir_to_path(parsed, unparse);
                    state = ABSOLUTE_WITH_N_DEPTH;
                    pathdepth++;
                }
                break;

            case ABSOLUTE_WITH_N_DEPTH:
                if (is_double_dot) {
                    // Go up one directory
                    *parsed = '\0';
                    parsed = trim_directory(path);
                    if (--pathdepth == 0)
                        state = ABSOLUTE_PATH_INIT;
                } else {
                    // Add normal directory
                    *parsed++ = '/';
                    parsed = append_subdir_to_path(parsed, unparse);
                    pathdepth++;
                }
                break;

            case RELATIVE_PATH_INIT:
                // Add component (either ".." or normal directory)
                parsed = append_subdir_to_path(parsed, unparse);
                if (is_double_dot)
                    state = RELATIVE_WITH_PARENT_REF;
                else {
                    state = RELATIVE_WITH_N_DEPTH;
                    pathdepth++;
                }
                break;

            case RELATIVE_WITH_N_DEPTH:
                if (is_double_dot) {
                    // Remove last directory
                    *parsed = '\0';
                    parsed = trim_directory(path);
                    if (--pathdepth == 0) {
                        state = (parsed == spath) ?
                               RELATIVE_PATH_INIT : RELATIVE_WITH_PARENT_REF;
                    }
                } else {
                    // Add normal directory
                    *parsed++ = '/';
                    parsed = append_subdir_to_path(parsed, unparse);
                    pathdepth++;
                }
                break;

            case RELATIVE_WITH_PARENT_REF:
                // Add component (preserve ".." or start counting depth)
                *parsed++ = '/';
                parsed = append_subdir_to_path(parsed, unparse);
                if (!is_double_dot) {
                    state = RELATIVE_WITH_N_DEPTH;
                    pathdepth = 1;
                }
                break;
        }

        unparse = unparse_next;
    }

    // Handle empty result - insert "." for current directory
    if (parsed == spath)
        *parsed++ = '.';

    // Null-terminate the result
    *parsed = '\0';
}
```

Key simplifications made:
- Consolidated similar state machine cases with clearer logic flow
- Added explanatory comments for each major section
- Simplified variable names and removed some intermediate variables
- Focused on the core path canonicalization algorithm
- Abstracted complex helper function details with descriptive comments
- Maintained the essential state machine logic for proper ".." handling