# mkdirs

## Location
[src/timezone/zic.c:3948-4005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3948-L4005)

## Overview
Creates necessary directory hierarchies for a given path, handling both ancestor-only and full path creation modes.

## Definition
```c
static void mkdirs(char const *argname, bool ancestors)
```

## Detailed Description
The `mkdirs` function ensures that all required directories in a path exist by creating any missing ones. It supports two modes of operation controlled by the `ancestors` parameter:

1. **Ancestor mode**: Creates only the parent directories, not the final path component
2. **Full path mode**: Creates all directories including the final path component

Key features include:
- Handles Unix-style forward slash path separators
- Skips root directory creation (assumes it exists)
- Uses optimistic directory creation - tries mkdir first, then handles errors
- Optimizes for EEXIST case by skipping directory existence check
- Provides cross-platform compatibility notes for Windows systems
- Handles concurrent directory creation by other processes gracefully
- Uses safe memory allocation via `ecpyalloc`

The function is essential for timezone compiler operations where output directories may not exist.

## Parameters / Member Variables
- `argname`: The file or directory path for which to ensure parent directories exist
- `ancestors`: Boolean flag - if true, create only ancestor directories; if false, create the full path

## Dependencies
- Functions called/Symbols referenced:
  - `[ecpyalloc](../e/ecpyalloc.md)`: Safe memory allocation and string copying function
  - `mkdir`: System call to create directories with MKDIR_UMASK permissions
  - [itsdir](../i/itsdir.md): Check if a path is an existing directory
  - [error](../e/error.md): Error reporting function
  - `strerror`: Convert errno to string
  - `strchr`: Standard C string search function
  - `free`: Standard C memory deallocation
- Called from (representative examples):
  - [change_directory](../c/change_directory.md): Directory change operations
  - [dolink](../d/dolink.md): File linking operations
  - [writezone](../w/writezone.md): Timezone data file writing

## Notes and Other Information
- Exits with EXIT_FAILURE on irrecoverable directory creation errors
- Ignores EEXIST errors as they indicate the directory already exists
- Modifies a local copy of the path string to avoid affecting the original
- Cross-platform design accounts for Windows drive letters and backslashes
- Called primarily after file operations fail with ENOENT to ensure directory structure
- Part of the zic (zone information compiler) file management system
- Uses MKDIR_UMASK for consistent directory permissions