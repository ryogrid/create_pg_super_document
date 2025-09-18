# first_dir_separator

## Location
src/port/path.c: 109 - 125

## Overview
A utility function that locates the first directory separator in a pathname, skipping any drive prefix on Windows systems.

## Definition
```c
char *first_dir_separator(const char *filename)
```

## Detailed Description
The first_dir_separator function searches for the first occurrence of a directory separator character in a given filename or path. It intelligently handles platform-specific path formats by:

1. **Skipping drive prefixes**: Uses skip_drive() to bypass Windows drive letters (C:) or UNC prefixes (\\\\server\\share) before searching
2. **Cross-platform separator detection**: Uses the IS_DIR_SEP macro to detect both forward slashes (/) and backslashes (\\) as appropriate for the platform
3. **Returning a modifiable pointer**: Uses unconstify() to return a non-const pointer to the found separator location

The function is commonly used in path parsing operations where you need to split a path into directory and filename components, or when validating that certain strings don't contain directory separators.

## Parameters / Member Variables
- `filename`: Input filename or path string to search for directory separators

## Dependencies
- Functions called/Symbols referenced:
  - skip_drive (to bypass drive prefixes)
  - IS_DIR_SEP (macro for checking directory separators)
  - unconstify (macro for casting away const qualifier)
- Called from (representative examples):
  - check_valid_extension_name (in src/backend/commands/extension.c)
  - check_valid_version_name (in src/backend/commands/extension.c)
  - expand_dynamic_library_name (in src/backend/utils/fmgr/dfmgr.c)
  - check_restricted_library_name (in src/backend/utils/fmgr/dfmgr.c)
  - substitute_libpath_macro (in src/backend/utils/fmgr/dfmgr.c)
  - find_in_dynamic_libpath (in src/backend/utils/fmgr/dfmgr.c)
  - load_libraries (in src/backend/utils/init/miscinit.c)
  - find_my_exec (in src/common/exec.c)

## Notes and Other Information
- Returns NULL if no directory separator is found in the path
- This is a public function available throughout the PostgreSQL codebase
- Particularly important for security checks in extension and library loading where directory separators in names could indicate path traversal attempts
- The function handles both absolute and relative paths correctly by skipping drive prefixes first
- Used extensively in dynamic library management and extension system for path validation