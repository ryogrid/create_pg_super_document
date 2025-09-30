# PathNameCreateTemporaryFile

## Location
[src/backend/storage/file/fd.c:1858-1897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1858-L1897)

## Overview
PathNameCreateTemporaryFile creates a new temporary file at a specified path with automatic resource management and temp file limit accounting.

## Definition

```c
File
PathNameCreateTemporaryFile(const char *path, bool error_on_failure)
```
## Detailed Description
This function creates a temporary file at the given path with comprehensive resource management features. Unlike automatically generated temporary files, this function allows specification of the exact file path while still providing PostgreSQL's temporary file management benefits.

Key features include:
- Subject to temp_file_limit quota enforcement
- Automatic closure at transaction end
- Resource ownership tracking
- Integration with PostgreSQL's file descriptor cache
- Optional error handling modes

The file is opened with O_RDWR | O_CREAT | O_TRUNC | PG_BINARY flags but deliberately omits O_EXCL to allow reuse of orphaned files. The function assumes the containing directory already exists and requires temporary file access to be enabled.

## Parameters / Member Variables
- : Full filesystem path where the temporary file should be created
- : If true, emits ERROR on failure; if false, returns invalid file handle silently

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md) (ensures resource tracking capacity)
  - [PathNameOpenFile](PathNameOpenFile.md) (performs actual file creation/opening)
  - [RegisterTemporaryFile](../R/RegisterTemporaryFile.md) (registers for automatic cleanup)
  - PG_BINARY (binary file mode constant)
  - FD_TEMP_FILE_LIMIT (flag for temp file limit accounting)
  - VfdCache (virtual file descriptor cache)

- Called from (representative examples):
  - [FileSetCreate](../F/FileSetCreate.md) (creates shared temp files for filesets)

## Notes and Other Information
- Files created this way are NOT automatically deleted on close, making them suitable for sharing between backends
- If the file is in the top-level temp directory, its name should start with PG_TEMP_FILE_PREFIX for proper cleanup
- Files in directories created with PathNameCreateTemporaryDir() don't require the prefix
- The function asserts that temporary_files_allowed is true before proceeding
- Uses resource owner tracking to ensure proper cleanup even in error scenarios
- Integrates with PostgreSQL's temp_file_limit mechanism for disk space management
- The absence of O_EXCL allows recovery from crashes that leave orphaned temp files

## Simplified Source

```c
File PathNameCreateTemporaryFile(const char *path, bool error_on_failure) {
    File file;

    // Ensure temporary file access is enabled and resource tracking available
    Assert(temporary_files_allowed);
    ResourceOwnerEnlarge(CurrentResourceOwner);

    // Create/open the file (allow reuse of orphaned files)
    file = PathNameOpenFile(path, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
    if (file <= 0) {
        if (error_on_failure) {
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not create temporary file \"%s\": %m", path)));
        } else {
            return file;  // Return failure indicator
        }
    }

    // Mark file for temp_file_limit accounting
    VfdCache[file].fdstate |= FD_TEMP_FILE_LIMIT;

    // Register for automatic close at transaction end
    RegisterTemporaryFile(file);

    return file;
}
```