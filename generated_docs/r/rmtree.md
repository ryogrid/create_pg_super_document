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
  -  - [String](../S/String.md) formatting
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

## Simplified Source

```c
// Simplified version of rmtree
bool rmtree(const char *path, bool rmtopdir) {
    char pathbuf[MAXPGPATH];
    DIR *dir;
    struct dirent *de;
    bool success = true;

    // Dynamic array to store subdirectory names for deferred recursion
    size_t subdirs_count = 0;
    size_t subdirs_capacity = 8;
    char **subdirs = (char **) palloc(sizeof(char *) * subdirs_capacity);

    // Open the directory
    dir = OPENDIR(path);
    if (dir == NULL) {
        pg_log_warning("could not open directory \"%s\": %m", path);
        return false;
    }

    // Process all directory entries
    while (errno = 0, (de = readdir(dir))) {
        // Skip current and parent directory entries
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) {
            continue;
        }

        // Build full path for this entry
        snprintf(pathbuf, sizeof(pathbuf), "%s/%s", path, de->d_name);

        // Handle entry based on its type
        switch (get_dirent_type(pathbuf, de, false, LOG_LEVEL)) {
            case PGFILETYPE_ERROR:
                // Error already logged, continue processing
                break;

            case PGFILETYPE_DIR:
                // Store subdirectory for later recursive processing
                if (subdirs_count == subdirs_capacity) {
                    subdirs = repalloc(subdirs, sizeof(char *) * subdirs_capacity * 2);
                    subdirs_capacity *= 2;
                }
                subdirs[subdirs_count++] = pstrdup(pathbuf);
                break;

            default:
                // Remove regular files and other non-directory entries
                if (unlink(pathbuf) != 0 && errno != ENOENT) {
                    pg_log_warning("could not remove file \"%s\": %m", pathbuf);
                    success = false;
                }
                break;
        }
    }

    // Check for directory reading errors
    if (errno != 0) {
        pg_log_warning("could not read directory \"%s\": %m", path);
        success = false;
    }

    CLOSEDIR(dir);

    // Recursively process subdirectories
    for (size_t i = 0; i < subdirs_count; ++i) {
        if (!rmtree(subdirs[i], true)) {
            success = false;
        }
        pfree(subdirs[i]);
    }

    // Remove the top directory if requested
    if (rmtopdir) {
        if (rmdir(path) != 0) {
            pg_log_warning("could not remove directory \"%s\": %m", path);
            success = false;
        }
    }

    pfree(subdirs);
    return success;
}
```

Key simplifications made:
- Added descriptive comments explaining the two-phase approach
- Used more descriptive variable names (success vs result, subdirs vs dirnames)
- Organized code into logical sections (open, process entries, recurse, cleanup)
- Clarified the file descriptor management strategy
- Preserved all error handling and memory management
- Emphasized the deferred recursion pattern for resource efficiency