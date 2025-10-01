# PathNameOpenTemporaryFile

## Location
[src/backend/storage/file/fd.c:1898-1928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1898-L1928)

## Overview
PathNameOpenTemporaryFile opens an existing temporary file that was previously created, typically by another backend process.

## Definition

```c
struct stat filestats;
```
## Detailed Description
This function opens an existing temporary file that was created using PathNameCreateTemporaryFile(), potentially by a different backend process. It is designed for sharing temporary files between cooperating PostgreSQL backends.

Key characteristics:
- Does not count against the caller's temp_file_limit (since the file was already accounted for when created)
- Automatically closed at transaction end but not deleted
- Gracefully handles missing files (returns invalid handle without error for ENOENT)
- Includes resource owner tracking and automatic cleanup registration
- Always opens files in binary mode by adding PG_BINARY to the specified mode

The function is particularly useful in parallel query execution and other scenarios where multiple backends need to access shared temporary data.

## Parameters / Member Variables
- : Full filesystem path to the existing temporary file to open
- : File access mode flags (e.g., O_RDONLY, O_RDWR), PG_BINARY is automatically added

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md) (ensures resource tracking capacity)
  - [PathNameOpenFile](PathNameOpenFile.md) (performs actual file opening)
  - [RegisterTemporaryFile](../R/RegisterTemporaryFile.md) (registers for automatic cleanup)
  - PG_BINARY (binary file mode constant)

- Called from (representative examples):
  - [FileSetOpen](../F/FileSetOpen.md) (opens shared temp files in filesets)

## Notes and Other Information
- The function specifically does NOT raise an error if the file doesn't exist (ENOENT), returning an invalid file handle instead
- Other error conditions (permissions, I/O errors, etc.) do trigger ERROR reports
- Files opened this way are automatically registered for closure at transaction end
- The function assumes temporary_files_allowed is enabled and asserts this condition
- Unlike PathNameCreateTemporaryFile, this function doesn't set FD_TEMP_FILE_LIMIT since the file was already accounted for during creation
- Designed for inter-backend cooperation in scenarios like parallel queries or shared work files
- The automatic addition of PG_BINARY ensures consistent binary mode operation across platforms

## Simplified Source

```c
File
PathNameOpenTemporaryFile(const char *path, int mode)
{
    File file;

    // Verify temporary file operations are allowed
    Assert(temporary_files_allowed);

    // Ensure resource tracking has space
    ResourceOwnerEnlarge(CurrentResourceOwner);

    // Open the file with binary mode
    file = PathNameOpenFile(path, mode | PG_BINARY);

    // Handle errors (except file not found, which is allowed)
    if (file <= 0 && errno != ENOENT) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open temporary file \"%s\": %m", path)));
    }

    // Register for automatic cleanup if successfully opened
    if (file > 0) {
        RegisterTemporaryFile(file);
    }

    return file;  // Valid file handle or <= 0 if failed/not found
}
```