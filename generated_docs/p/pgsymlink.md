# pgsymlink

## Location
[src/port/dirmod.c:219-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/dirmod.c#L219-L308)

## Overview
A Windows-specific function that creates symbolic links by implementing junction points using Win32 reparse points, providing cross-platform symbolic link functionality.

## Definition

```c
int
pgsymlink(const char *oldpath, const char *newpath)
```
## Detailed Description
The  function implements symbolic link creation on Windows by utilizing NTFS junction points through the Win32 reparse point mechanism. Since Windows (especially older versions) lacks native symbolic link support, this function provides a compatible alternative using junction points, which offer similar functionality.

The function creates a directory at the target location, then configures it as a reparse point that redirects to the source path. It handles path format conversion from Unix-style forward slashes to Windows-style backslashes, and ensures the target path is in the proper Win32 native format (prefixed with  if not already present).

The implementation uses low-level Win32 APIs including  with reparse point flags,  with , and proper Unicode conversion for path storage. Error handling includes detailed error reporting and cleanup of partially created structures.

## Parameters / Member Variables
- `*oldpath`: Source path that the junction point should target
- `*newpath`: Path where the new junction point should be created
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

## Simplified Source

```c
int
pgsymlink(const char *oldpath, const char *newpath)
{
    HANDLE dirhandle;
    DWORD len;
    char buffer[MAX_PATH * sizeof(WCHAR) + offsetof(REPARSE_JUNCTION_DATA_BUFFER, PathBuffer)];
    char nativeTarget[MAX_PATH];
    char *p = nativeTarget;
    REPARSE_JUNCTION_DATA_BUFFER *reparseBuf = (REPARSE_JUNCTION_DATA_BUFFER *) buffer;

    // Create directory for junction point
    CreateDirectory(newpath, 0);
    dirhandle = CreateFile(newpath, GENERIC_READ | GENERIC_WRITE,
                           0, 0, OPEN_EXISTING,
                           FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, 0);

    if (dirhandle == INVALID_HANDLE_VALUE)
    {
        _dosmaperr(GetLastError());
        return -1;
    }

    // Convert path to native Win32 format with \\?\\ prefix
    if (memcmp("\\\\?\\", oldpath, 4) != 0)
        snprintf(nativeTarget, sizeof(nativeTarget), "\\\\?\\%s", oldpath);
    else
        strlcpy(nativeTarget, oldpath, sizeof(nativeTarget));

    // Convert forward slashes to backslashes
    while ((p = strchr(p, '/')) != NULL)
        *p++ = '\\';

    // Set up reparse point data structure
    len = strlen(nativeTarget) * sizeof(WCHAR);
    reparseBuf->ReparseTag = IO_REPARSE_TAG_MOUNT_POINT;
    reparseBuf->ReparseDataLength = len + 12;
    reparseBuf->Reserved = 0;
    reparseBuf->SubstituteNameOffset = 0;
    reparseBuf->SubstituteNameLength = len;
    reparseBuf->PrintNameOffset = len + sizeof(WCHAR);
    reparseBuf->PrintNameLength = 0;

    // Convert to wide character string
    MultiByteToWideChar(CP_ACP, 0, nativeTarget, -1,
                        reparseBuf->PathBuffer, MAX_PATH);

    // Create the junction point via device control
    if (!DeviceIoControl(dirhandle,
                         CTL_CODE(FILE_DEVICE_FILE_SYSTEM, 41, METHOD_BUFFERED, FILE_ANY_ACCESS),
                         reparseBuf,
                         reparseBuf->ReparseDataLength + REPARSE_JUNCTION_DATA_BUFFER_HEADER_SIZE,
                         0, 0, &len, 0))
    {
        // Error handling and cleanup
        _dosmaperr(GetLastError());
        int save_errno = errno;

        // Report error with formatted message
        LPSTR msg;
        FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                      FORMAT_MESSAGE_FROM_SYSTEM,
                      NULL, GetLastError(),
                      MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
                      (LPSTR) &msg, 0, NULL);

        // Backend vs frontend error reporting
#ifndef FRONTEND
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not set junction for \"%s\": %s",
                             nativeTarget, msg)));
#else
        fprintf(stderr, _("could not set junction for \"%s\": %s\n"),
                nativeTarget, msg);
#endif
        LocalFree(msg);

        // Clean up on failure
        CloseHandle(dirhandle);
        RemoveDirectory(newpath);
        errno = save_errno;
        return -1;
    }

    CloseHandle(dirhandle);
    return 0;  // Success
}
```