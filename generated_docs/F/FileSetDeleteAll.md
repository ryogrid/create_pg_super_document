# FileSetDeleteAll

## Location
[src/backend/storage/file/fileset.c:150-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fileset.c#L150-L171)

## Overview
Deletes all files and directories associated with a FileSet across all configured tablespaces, providing complete cleanup of the fileset's resources.

## Definition

```c
void
FileSetDeleteAll(FileSet *fileset)
```
## Detailed Description
FileSetDeleteAll provides comprehensive cleanup for a FileSet by removing all directories (and implicitly all files within them) that were created for the fileset across all configured tablespaces. This function implements a complete resource cleanup strategy, ensuring that no temporary files or directory structures remain after a FileSet is no longer needed.

The function iterates through all tablespaces that were configured during FileSetInit, constructing the directory path for each tablespace and removing the entire directory tree. This operation removes both the files within each directory and the directory structure itself, providing thorough cleanup.

The function is designed to be safe for use in error cleanup paths, as it does not fail on errors but may generate LOG messages for I/O errors. This makes it suitable for use in exception handlers and cleanup routines where partial failures should not prevent the cleanup process from continuing.

## Parameters / Member Variables
- `*fileset`: Pointer to the FileSet structure whose resources should be completely cleaned up
## Dependencies
- Functions called/Symbols referenced:
  - [FileSetPath](FileSetPath.md): Constructs the directory path for the fileset within a specific tablespace
  - [PathNameDeleteTemporaryDir](../P/PathNameDeleteTemporaryDir.md): Removes the entire directory tree at the specified path
  - MAXPGPATH: Maximum path length constant

- Called from (representative examples):
  - [logicalrep_worker_onexit](../l/logicalrep_worker_onexit.md): Used in logical replication worker cleanup during process exit
  - [SharedFileSetDeleteAll](../S/SharedFileSetDeleteAll.md): Used to clean up shared filesets
  - [SharedFileSetOnDetach](../S/SharedFileSetOnDetach.md): Used when detaching from shared memory segments

## Notes and Other Information
- Removes entire directory trees, not just individual files
- Operates across all tablespaces configured in the FileSet
- Designed for safe use in error cleanup paths - does not throw errors on failure
- May generate LOG messages for I/O errors but continues execution
- More efficient than calling FileSetDelete for each individual file
- Essential for preventing resource leaks and disk space accumulation
- The function assumes the FileSet structure is valid and properly initialized
- Directory removal is recursive, eliminating all files and subdirectories
- Used primarily during process termination, error cleanup, and resource deallocation scenarios
- Complements FileSetDelete for complete resource management - [FileSetDelete](FileSetDelete.md) handles individual files while FileSetDeleteAll handles bulk cleanup

## Simplified Source

```c
void FileSetDeleteAll(FileSet *fileset)
{
    char dirpath[MAXPGPATH];

    // Delete directories in each configured tablespace
    for (int i = 0; i < fileset->ntablespaces; ++i) {
        // Build the directory path for this tablespace
        FileSetPath(dirpath, fileset, fileset->tablespaces[i]);

        // Remove the entire directory tree (safe for error cleanup)
        PathNameDeleteTemporaryDir(dirpath);
    }
}
```