# pgreadlink

## Location
[src/port/dirmod.c:309-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/dirmod.c#L309-L422)

## Overview
A Windows-specific function that reads the target path of symbolic links by examining junction points using Win32 reparse point mechanisms, providing cross-platform readlink functionality.

## Definition

```c
int
pgreadlink(const char *path, char *buf, size_t size)
```
## Detailed Description
The  function implements symbolic link target reading on Windows by interrogating NTFS junction points through the Win32 reparse point system. This function serves as the counterpart to , allowing applications to determine where a junction point refers to.

The function first verifies that the specified path exists and has the  attribute set, indicating it's a reparse point (junction point). It then opens the path with reparse point access flags and uses  with  to retrieve the junction point's target information.

After extracting the raw reparse data, the function validates that it's specifically a mount point type junction (using ), converts the Unicode target path back to multi-byte format, and performs path normalization to remove Windows-specific prefixes like  for drive-absolute paths.

The implementation includes comprehensive error handling with detailed error reporting and automatic cleanup of resources.

## Parameters / Member Variables
- `*path`: Path to the junction point/symbolic link to read
- `*buf`: Buffer to store the target path
- `size`: Size of the output buffer
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
- [Path](../P/Path.md) normalization only handles drive-absolute paths; other exotic Windows path formats are returned as-is
- Requires the target file/directory to have  set
- Uses  to open the reparse point itself rather than following it
- The function performs Unicode conversion to ensure proper handling of international characters in paths
- Complements  by providing the inverse operation for junction point introspection

## Simplified Source

```c
int
pgreadlink(const char *path, char *buf, size_t size)
{
    DWORD attr;
    HANDLE h;
    char buffer[MAX_PATH * sizeof(WCHAR) + offsetof(REPARSE_JUNCTION_DATA_BUFFER, PathBuffer)];
    REPARSE_JUNCTION_DATA_BUFFER *reparseBuf = (REPARSE_JUNCTION_DATA_BUFFER *) buffer;
    DWORD len;
    int r;

    // Check if path exists and is a reparse point
    attr = GetFileAttributes(path);
    if (attr == INVALID_FILE_ATTRIBUTES)
    {
        _dosmaperr(GetLastError());
        return -1;
    }
    if ((attr & FILE_ATTRIBUTE_REPARSE_POINT) == 0)
    {
        errno = EINVAL;
        return -1;
    }

    // Open the reparse point for reading
    h = CreateFile(path, GENERIC_READ,
                   FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
                   OPEN_EXISTING,
                   FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, 0);
    if (h == INVALID_HANDLE_VALUE)
    {
        _dosmaperr(GetLastError());
        return -1;
    }

    // Get reparse point data
    if (!DeviceIoControl(h, FSCTL_GET_REPARSE_POINT,
                         NULL, 0, (LPVOID) reparseBuf,
                         sizeof(buffer), &len, NULL))
    {
        // Error handling with formatted message
        LPSTR msg;
        FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                      FORMAT_MESSAGE_FROM_SYSTEM,
                      NULL, GetLastError(),
                      MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
                      (LPSTR) &msg, 0, NULL);

#ifndef FRONTEND
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not get junction for \"%s\": %s", path, msg)));
#else
        fprintf(stderr, _("could not get junction for \"%s\": %s\n"), path, msg);
#endif
        LocalFree(msg);
        CloseHandle(h);
        errno = EINVAL;
        return -1;
    }
    CloseHandle(h);

    // Verify this is a mount point junction
    if (reparseBuf->ReparseTag != IO_REPARSE_TAG_MOUNT_POINT)
    {
        errno = EINVAL;
        return -1;
    }

    // Convert Unicode path to multi-byte
    r = WideCharToMultiByte(CP_ACP, 0, reparseBuf->PathBuffer, -1,
                            buf, size, NULL, NULL);
    if (r <= 0)
    {
        errno = EINVAL;
        return -1;
    }

    // r includes null terminator, subtract it
    r -= 1;

    // Strip \\?\\ prefix from drive-absolute paths (C:\path)
    // This undoes pgsymlink() transformation for user-friendly output
    if (r >= 7 &&
        buf[0] == '\\' && buf[1] == '?' && buf[2] == '?' && buf[3] == '\\' &&
        isalpha(buf[4]) && buf[5] == ':' && buf[6] == '\\')
    {
        memmove(buf, buf + 4, strlen(buf + 4) + 1);
        r -= 4;
    }

    return r;  // Length of target path
}
```