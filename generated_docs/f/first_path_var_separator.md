# first_path_var_separator

## Location
[src/port/path.c:126-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L126-L143)

## Overview
A utility function that finds the first path variable separator character in a path list string, used for parsing environment variables like PATH.

## Definition
```c
char *first_path_var_separator(const char *pathlist)
```

## Detailed Description
The first_path_var_separator function locates the first occurrence of a path variable separator in a given path list string. Path variable separators are platform-specific characters used to separate multiple paths in environment variables:

- **Unix-like systems**: Uses colon (:) as the path separator (e.g., "/usr/bin:/bin:/usr/local/bin")
- **Windows systems**: Uses semicolon (;) as the path separator (e.g., "C:\\Windows\\System32;C:\\Windows")

The function searches through the entire pathlist string without needing to skip drive prefixes (unlike directory separator functions) since path variable separators don't conflict with drive specifications.

This function is essential for parsing environment variables like PATH, LD_LIBRARY_PATH, or PostgreSQL-specific path variables that contain multiple directory paths.

## Parameters / Member Variables
- `pathlist`: Input string containing a list of paths separated by platform-specific path variable separators

## Dependencies
- Functions called/Symbols referenced:
  - IS_PATH_VAR_SEP (macro for checking path variable separators)
  - unconstify (macro for casting away const qualifier)
- Called from (representative examples):
  - [find_in_dynamic_libpath](find_in_dynamic_libpath.md) (in src/backend/utils/fmgr/dfmgr.c)
  - [find_my_exec](find_my_exec.md) (in src/common/exec.c)

## Notes and Other Information
- Returns NULL if no path variable separator is found in the pathlist
- This is a public function available throughout the PostgreSQL codebase
- Does not require drive prefix skipping since path variable separators are distinct from drive specifications
- Commonly used in library path resolution and executable finding operations
- The comment "skip_drive is not needed" in the source emphasizes that this function works directly on the input without platform-specific path preprocessing
- Essential for parsing PATH-like environment variables when searching for executables or libraries

## Simplified Source

```c
// Simplified version of first_path_var_separator
char *first_path_var_separator(const char *pathlist) {
    // Iterate through each character in the path list
    for (const char *p = pathlist; *p; p++) {
        // Check if current character is a path variable separator (':' on Unix, ';' on Windows)
        if (IS_PATH_VAR_SEP(*p)) {
            // Return pointer to the first separator found
            return unconstify(char *, p);
        }
    }

    // No separator found - return NULL
    return NULL;
}
```

Key simplifications made:
- Combined variable declaration with loop initialization for clarity
- Added descriptive comments explaining each logical step
- Emphasized the platform-specific nature of path separators
- Maintained the essential search algorithm and return logic
- Preserved the original function's simplicity while improving readability