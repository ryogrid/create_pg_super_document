# set_stack_entry_location

## Location
src/backend/utils/error/elog.c: 799 - 828

## Overview
Stores source code location information (filename, line number, and function name) in an error data stack entry, normalizing the filename path for consistency across different build environments.

## Definition
```c
static void set_stack_entry_location(ErrorData *edata,
                                     const char *filename, int lineno,
                                     const char *funcname)
```

## Detailed Description
This function captures and stores debugging information about where an error occurred in the source code. It takes the standard compiler-provided macros (__FILE__, __LINE__, and __func__) and stores them in the ErrorData structure for later use in error reporting.

A key feature is the filename normalization process: the function strips directory paths from the filename to keep only the base name. This ensures consistent behavior regardless of build paths or compiler differences in how much of the full path is included in __FILE__. The function handles both Unix-style forward slashes and Windows-style backslashes when extracting the base filename.

## Parameters / Member Variables
- `edata`: ErrorData * - Pointer to the error data structure to populate with location information
- `filename`: const char * - Source filename (typically from __FILE__ macro), may include full path
- `lineno`: int - Line number where error occurred (typically from __LINE__ macro)  
- `funcname`: const char * - Function name where error occurred (typically from __func__ macro)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - strrchr (standard library function for finding last occurrence of character)

- Called from (representative examples):
  - [errfinish](../e/errfinish.md) (src/backend/utils/error/elog.c:488)
  - [errsave_finish](../e/errsave_finish.md) (src/backend/utils/error/elog.c:711)

## Notes and Other Information
- The function is static and only used internally within the error handling subsystem
- Filename normalization ensures consistent behavior across different compilers and build systems
- Handles both Unix (/) and Windows (\\) path separators for cross-platform compatibility
- The normalized filename excludes directory paths to make error messages more readable and build-path independent
- Used primarily for debugging and error reporting to help developers locate where errors originated
- Does not allocate memory; stores pointers to the provided strings directly in the ErrorData structure