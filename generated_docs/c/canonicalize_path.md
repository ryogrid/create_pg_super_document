# canonicalize_path

## Location
[src/port/path.c:336-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L336-L342)

## Overview
Cleans up and normalizes file system paths by applying various transformations to make paths consistent and canonical.

## Definition

```c
void
canonicalize_path(char *path)
```
## Detailed Description
The  function is a convenience wrapper around  that normalizes file system paths by applying multiple cleanup operations. It modifies the path in-place and performs the following transformations:

- Converts Win32 paths to use Unix-style forward slashes
- Removes trailing quotes on Win32 systems
- Removes trailing slashes from paths
- Removes duplicate adjacent path separators
- Removes '.' components (unless the path reduces to only '.')
- Processes '..' components, removing them when possible

This function assumes the input path is in a server-safe encoding and uses  as the encoding parameter when calling the underlying  function.

## Parameters / Member Variables
- : A null-terminated string containing the file system path to be canonicalized. The path is modified in-place.

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
- Called from (representative examples):
  -  (src/backend/commands/tablespace.c:236)
  -  (src/backend/commands/variable.c:1055)
  -  (src/backend/utils/adt/genfile.c:59)
  -  (src/common/exec.c:207)
  -  (src/port/path.c:767)
  -  (src/port/path.c:891)

## Notes and Other Information
- This function is part of PostgreSQL's portable path utilities located in src/port/path.c
- The function modifies the input string in-place, so callers should ensure they have a modifiable copy if the original path needs to be preserved
- This is the encoding-unaware variant - use  directly when dealing with paths that may contain characters in specific encodings
- Widely used throughout PostgreSQL codebase for path normalization in various contexts including tablespace management, configuration file handling, and executable path resolution

## Simplified Source

```c
// Simplified version of canonicalize_path and canonicalize_path_enc
void canonicalize_path(char *path) {
    // Step 1: Convert Windows backslashes to forward slashes
    #ifdef WIN32
    convert_backslashes_to_forward_slashes(path);
    remove_trailing_quote_on_windows(path);
    #endif

    // Step 2: Remove trailing slash (but keep leading slash)
    remove_trailing_separator(path);

    // Step 3: Remove duplicate adjacent separators like "a//b" -> "a/b"
    char *read_pos = path;
    char *write_pos = path;
    bool prev_was_slash = false;

    while (*read_pos) {
        if (*read_pos == '/' && prev_was_slash) {
            // Skip duplicate slash
            read_pos++;
            continue;
        }
        *write_pos = *read_pos;
        prev_was_slash = (*read_pos == '/');
        read_pos++;
        write_pos++;
    }
    *write_pos = '\0';

    // Step 4: Process "." and ".." components
    bool is_absolute = (path[0] == '/');
    char *components[MAX_PATH_COMPONENTS];
    int component_count = 0;

    // Split path into components
    char *token = strtok(path_after_drive_prefix, "/");
    while (token != NULL) {
        if (strcmp(token, ".") == 0) {
            // Ignore "." components
        } else if (strcmp(token, "..") == 0) {
            if (component_count > 0 && is_absolute) {
                // Remove last component for absolute paths
                component_count--;
            } else if (!is_absolute) {
                // Keep ".." for relative paths that can't be resolved
                components[component_count++] = token;
            }
        } else {
            // Regular directory name
            components[component_count++] = token;
        }
        token = strtok(NULL, "/");
    }

    // Step 5: Rebuild the cleaned path
    if (is_absolute) {
        strcpy(path, "/");
    } else {
        path[0] = '\0';
    }

    for (int i = 0; i < component_count; i++) {
        if (i > 0 || is_absolute) {
            strcat(path, "/");
        }
        strcat(path, components[i]);
    }

    // Step 6: Handle empty path case
    if (path[0] == '\0') {
        strcpy(path, ".");
    }
}
```

Key simplifications made:
- Removed complex state machine logic and replaced with simpler component-based processing
- Abstracted platform-specific operations into conceptual functions
- Consolidated the duplicate separator removal into a straightforward loop
- Simplified the ".." processing by using an array to track path components
- Removed detailed encoding handling (assumes server-safe encoding)
- Focused on the main algorithm flow rather than edge case optimizations
- Combined the logic from both `canonicalize_path` and `canonicalize_path_enc` functions