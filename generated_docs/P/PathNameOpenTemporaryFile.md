# PathNameOpenTemporaryFile

## Location
src/backend/storage/file/fd.c: 1898 - 1928

## Overview
PathNameOpenTemporaryFile opens an existing temporary file that was previously created, typically by another backend process.

## Definition


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
  - ResourceOwnerEnlarge (ensures resource tracking capacity)
  - PathNameOpenFile (performs actual file opening)
  - RegisterTemporaryFile (registers for automatic cleanup)
  - PG_BINARY (binary file mode constant)

- Called from (representative examples):
  - FileSetOpen (opens shared temp files in filesets)

## Notes and Other Information
- The function specifically does NOT raise an error if the file doesn't exist (ENOENT), returning an invalid file handle instead
- Other error conditions (permissions, I/O errors, etc.) do trigger ERROR reports
- Files opened this way are automatically registered for closure at transaction end
- The function assumes temporary_files_allowed is enabled and asserts this condition
- Unlike PathNameCreateTemporaryFile, this function doesn't set FD_TEMP_FILE_LIMIT since the file was already accounted for during creation
- Designed for inter-backend cooperation in scenarios like parallel queries or shared work files
- The automatic addition of PG_BINARY ensures consistent binary mode operation across platforms