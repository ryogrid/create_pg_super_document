# fsync_parent_path

## Location
[src/common/file_utils.c:434-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L434-L460)

## Overview
A static function that synchronizes the parent directory of a given file or directory path to ensure filesystem metadata persistence.

## Definition

```c
int
fsync_parent_path(const char *fname)
```
## Detailed Description
 extracts the parent directory path from the given filename and performs an fsync operation on it. This is crucial for ensuring that directory metadata changes (such as file creation, deletion, or renaming) are persistently written to disk, which is essential for crash recovery and ACID guarantees. The function handles the special case where the input is just a filename without a directory path by treating it as the current directory ("."). It uses  to extract the parent path and  to perform the actual synchronization.

## Parameters / Member Variables
- : The file or directory path whose parent directory should be synchronized
- : Error reporting level to use when logging errors (e.g., ERROR, WARNING, LOG)

## Dependencies
- Functions called/Symbols referenced:
  - [strlcpy](../s/strlcpy.md)
  - [get_parent_directory](../g/get_parent_directory.md)
  - [fsync_fname_ext](fsync_fname_ext.md)
- Called from (representative examples):
  - AllocateDesc
  - [durable_rename](../d/durable_rename.md)
  - [durable_unlink](../d/durable_unlink.md)

## Notes and Other Information
This is a static function in the backend storage subsystem, indicating it's an internal implementation detail for file durability operations. There's also a public version in src/common/file_utils.c with a simpler interface (without elevel parameter) that's used by client-side utilities like pg_basebackup. The function is essential for ensuring that filesystem metadata operations survive system crashes, particularly important for database consistency during file operations like creating new database files or renaming existing ones.

## Simplified Source

```c
// Simplified version of fsync_parent_path
static int fsync_parent_path(const char *fname, int elevel) {
    char parentpath[MAXPGPATH];

    // Extract parent directory from the file path
    strlcpy(parentpath, fname, MAXPGPATH);
    get_parent_directory(parentpath);

    // Handle case where input is just a filename (no directory component)
    if (strlen(parentpath) == 0) {
        strlcpy(parentpath, ".", MAXPGPATH);  // Use current directory
    }

    // Fsync the parent directory to ensure metadata persistence
    if (fsync_fname_ext(parentpath, true, false, elevel) != 0) {
        return -1;
    }

    return 0;
}
```

Key simplifications made:
- Added clear comments explaining each step
- Preserved the essential parent directory extraction logic
- Maintained the special case handling for filenames without paths
- Kept the critical fsync operation for directory metadata persistence
- Function is already quite simple, minimal changes needed