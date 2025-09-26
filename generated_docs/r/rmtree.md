# rmtree

## Location
[src/common/rmtree.c:50-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/rmtree.c#L50-L132)

## Overview
Recursively deletes a directory tree, optionally including the top-level directory itself. This is a utility function used throughout PostgreSQL for cleanup operations.

## Definition

```c
struct dirent *de;
```
## Detailed Description
The  function performs a recursive deletion of an entire directory tree. It's designed to safely remove directories and their contents without consuming excessive file descriptors by deferring subdirectory recursion until after the current directory is closed.

The function implements a two-phase approach:
1. **First pass**: Opens the directory, reads all entries, and immediately deletes files while collecting subdirectory names for later processing
2. **Second pass**: Recursively calls itself on collected subdirectories

This design ensures that only one file descriptor is used at any given time during the recursive operation, making it suitable for deep directory trees without exhausting system resources.

The function provides comprehensive error logging and continues processing even when individual operations fail, returning an overall success/failure status.

## Parameters
- : The path to the directory to be removed. Must point to a valid directory.
- : Boolean flag controlling whether the top-level directory itself should be removed after its contents are deleted. If false, only the directory's contents are removed.

## Dependencies
- Functions called/Symbols referenced:
  -  - Opens directory for reading
  -  - Reads directory entries
  -  - Closes directory handle
  -  - Determines file type of directory entry
  -  - Removes regular files
  -  - Removes empty directories
  - // - PostgreSQL memory management functions
  -  - PostgreSQL string duplication
  -  - PostgreSQL logging function
  -  - String formatting
  -  - Recursive self-call

- Called from (representative examples):
  -  - Database relocation operations
  -  - Tablespace cleanup
  -  - Replication slot cleanup
  -  - Application cleanup routines in initdb and pg_basebackup
  -  - pg_upgrade cleanup operations

## Notes and Other Information
- **File Descriptor Management**: The function carefully manages file descriptors by deferring subdirectory recursion, preventing resource exhaustion during deep recursions
- **Error Handling**: Continues processing even when individual file/directory operations fail, providing comprehensive error reporting via 
- **Memory Management**: Uses PostgreSQL's memory allocation functions and properly cleans up allocated memory
- **Atomic Operations**: The function is not atomic - partial deletions can occur if errors are encountered partway through
- **Cross-Platform Compatibility**: Uses PostgreSQL's portable directory handling macros (, )
- **Usage Pattern**: Commonly used in cleanup and error recovery scenarios throughout PostgreSQL, particularly in database management operations and utility programs
- **Return Value**: Returns  for complete success,  if any operation failed (with details already logged)