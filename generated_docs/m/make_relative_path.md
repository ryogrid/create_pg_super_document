# make_relative_path

## Location
[src/port/path.c:737-805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L737-L805)

## Overview
Creates a relative path from a target directory to support relocation of PostgreSQL installation trees by computing paths based on the actual executable location.

## Definition

```c
static void
make_relative_path(char *ret_path, const char *target_path,
				   const char *bin_path, const char *my_exec_path)
```
## Detailed Description
The  function is a core component of PostgreSQL's installation relocation support. It allows PostgreSQL installations to be moved to different directories while maintaining correct relative paths between components.

The function works by:
1. Finding the common prefix between the compiled-in target path and bin path
2. Extracting the remainder of the bin path (the "tail")
3. Checking if this tail matches the corresponding part of the actual executable path
4. If matched, constructing a new path by replacing the common prefix with the actual installation prefix
5. If no match, falling back to the original target path

For example, if PostgreSQL was compiled with  as the bin directory and  as the share directory, but is actually installed in , the function will correctly map the share directory to .

## Parameters / Member Variables
- : Output buffer (must be MAXPGPATH size) to store the resulting relative path
- : The compiled-in path to the directory we want to find (e.g., share directory)
- : The compiled-in path to the directory of executables
- : The actual location of the current executable

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to check directory separators
  -  - Safe string copying function
  -  - Removes the last path component
  -  - Normalizes path format
  -  - Directory-aware string comparison
  -  - Removes trailing path separators
  -  - Safely joins path components

- Called from (representative examples):
  -  - Getting shared data directory
  -  - Getting configuration directory
  -  - Getting header file directory
  -  - Getting library directory
  - Various other  functions for different PostgreSQL directories

## Notes and Other Information
- This is a static function, only accessible within src/port/path.c
- Critical for PostgreSQL's portability and ability to run from relocated installations
- Handles cross-platform path differences through the use of directory-aware helper functions
- Falls back gracefully to the original compiled-in path if relocation logic fails
- Used extensively by PostgreSQL utilities to find installation directories relative to the executable location
- The algorithm requires that the common prefix ends on a directory separator to avoid partial directory name matches

## Simplified Source

```c
// Simplified version of make_relative_path
static void make_relative_path(char *ret_path, const char *target_path,
                              const char *bin_path, const char *my_exec_path) {
    int prefix_len = 0;
    int i;

    // Step 1: Find common prefix between target_path and bin_path
    for (i = 0; target_path[i] && bin_path[i]; i++) {
        if (IS_DIR_SEP(target_path[i]) && IS_DIR_SEP(bin_path[i])) {
            prefix_len = i + 1;  // Mark end of directory
        } else if (target_path[i] != bin_path[i]) {
            break;  // Paths diverge
        }
    }

    // No common prefix found - use original target
    if (prefix_len == 0) {
        goto no_match;
    }

    // Step 2: Prepare executable path for comparison
    strlcpy(ret_path, my_exec_path, MAXPGPATH);
    trim_directory(ret_path);      // Remove executable name
    canonicalize_path(ret_path);   // Normalize path format

    // Step 3: Check if bin_path tail matches exec_path tail
    int tail_len = strlen(bin_path) - prefix_len;
    int tail_start = strlen(ret_path) - tail_len;

    if (tail_start > 0 &&
        IS_DIR_SEP(ret_path[tail_start - 1]) &&
        dir_strcmp(ret_path + tail_start, bin_path + prefix_len) == 0) {

        // Step 4: Build relocated path
        ret_path[tail_start] = '\0';  // Truncate at match point
        trim_trailing_separator(ret_path);
        join_path_components(ret_path, ret_path, target_path + prefix_len);
        canonicalize_path(ret_path);
        return;
    }

no_match:
    // Step 5: Fallback to original target path
    strlcpy(ret_path, target_path, MAXPGPATH);
    canonicalize_path(ret_path);
}
```

Key simplifications made:
- Removed detailed comments and consolidated into step-by-step flow
- Simplified variable declarations
- Added clear step markers for the main algorithm phases
- Preserved the core logic: prefix finding, tail matching, path construction, and fallback
- Maintained all essential error handling and edge cases