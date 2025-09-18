# pgreadlink

## Location
src/port/dirmod.c: 309 - 422

## Overview
A Windows-specific function that reads the target path of symbolic links by examining junction points using Win32 reparse point mechanisms, providing cross-platform readlink functionality.

## Definition


## Detailed Description
The  function implements symbolic link target reading on Windows by interrogating NTFS junction points through the Win32 reparse point system. This function serves as the counterpart to , allowing applications to determine where a junction point refers to.

The function first verifies that the specified path exists and has the  attribute set, indicating it's a reparse point (junction point). It then opens the path with reparse point access flags and uses  with  to retrieve the junction point's target information.

After extracting the raw reparse data, the function validates that it's specifically a mount point type junction (using ), converts the Unicode target path back to multi-byte format, and performs path normalization to remove Windows-specific prefixes like  for drive-absolute paths.

The implementation includes comprehensive error handling with detailed error reporting and automatic cleanup of resources.

## Parameters / Member Variables
- : Path to the junction point/symbolic link to read
- : Buffer to store the target path
- : Size of the output buffer

## Dependencies
- Functions called/Symbols referenced:
  -  (Win32 file attribute retrieval)
  -  (Windows error code mapping)
  -  (Win32 file/directory handle creation)
  -  (low-level device control for reparse point data)
  -  (Windows error message formatting)
  -  (Windows memory deallocation)
  -  (Win32 handle cleanup)
  -  (Unicode to multi-byte string conversion)
  -  (character classification)
  -  (memory block movement)
- Called from (representative examples):
  - Cross-platform code requiring symbolic link target resolution
  - File system traversal and analysis utilities

## Notes and Other Information
- This function is Windows-specific and only compiled on Win32 platforms
- Returns the length of the target path on success (excluding null terminator), -1 on failure
- The function only works with junction points created as mount point reparse points
- Automatically strips the  prefix from drive-absolute paths to provide user-friendly output
- Sets errno to EINVAL for various error conditions including non-reparse points and invalid reparse data
- Implements both frontend and backend error reporting mechanisms
- The output buffer must be large enough to hold the converted target path
- Path normalization only handles drive-absolute paths; other exotic Windows path formats are returned as-is
- Requires the target file/directory to have  set
- Uses  to open the reparse point itself rather than following it
- The function performs Unicode conversion to ensure proper handling of international characters in paths
- Complements  by providing the inverse operation for junction point introspection