# pgsymlink

## Location
src/port/dirmod.c: 219 - 308

## Overview
A Windows-specific function that creates symbolic links by implementing junction points using Win32 reparse points, providing cross-platform symbolic link functionality.

## Definition


## Detailed Description
The  function implements symbolic link creation on Windows by utilizing NTFS junction points through the Win32 reparse point mechanism. Since Windows (especially older versions) lacks native symbolic link support, this function provides a compatible alternative using junction points, which offer similar functionality.

The function creates a directory at the target location, then configures it as a reparse point that redirects to the source path. It handles path format conversion from Unix-style forward slashes to Windows-style backslashes, and ensures the target path is in the proper Win32 native format (prefixed with  if not already present).

The implementation uses low-level Win32 APIs including  with reparse point flags,  with , and proper Unicode conversion for path storage. Error handling includes detailed error reporting and cleanup of partially created structures.

## Parameters / Member Variables
- : Source path that the junction point should target
- : Path where the new junction point should be created

## Dependencies
- Functions called/Symbols referenced:
  -  (Win32 directory creation)
  -  (Win32 file/directory handle creation)
  -  (Windows error code mapping)
  -  (safe string copying)
  -  (character search in string)
  -  (string encoding conversion)
  -  (low-level device control)
  -  (Windows error message formatting)
  -  (Win32 handle cleanup)
  -  (directory removal on failure)
- Called from (representative examples):
  - Cross-platform code requiring symbolic link functionality
  - File system abstraction layers

## Notes and Other Information
- This function is Windows-specific and only compiled on Win32 platforms
- Returns 0 on success, -1 on failure with errno set appropriately
- Junction points are created by setting up a  structure with proper metadata
- The function converts Unix-style forward slashes to Windows backslashes automatically
- Implements proper error handling with detailed error messages using 
- On failure, performs cleanup by closing handles and removing the partially created directory
- Uses the  tag to identify the reparse point type
- The implementation includes both frontend and backend error reporting mechanisms
- Junction points created by this function can be removed using  or the  function
- The path conversion ensures compatibility with Win32 native path format requirements
- Reference implementation details are available at: http://www.codeproject.com/KB/winsdk/junctionpoints.aspx