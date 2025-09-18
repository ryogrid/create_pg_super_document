# cleanup_path

## Location
src/port/path.c: 257 - 284

## Overview
Cleans up file paths on Windows by converting them to short filenames (8.3 format) and replacing backslashes with forward slashes to ensure compatibility with both cmd.exe and MSYS environments.

## Definition
```c
void cleanup_path(char *path)
```

## Detailed Description
This function performs Windows-specific path cleanup operations to ensure paths work reliably with different Windows command environments. The cleanup process involves two steps: first, it attempts to convert the path to its short filename equivalent using Windows' GetShortPathName() API, which creates space-free 8.3 format paths (e.g., "C:/Progra~1/" instead of "C:/Program Files/"). Second, it converts any backslashes to forward slashes using debackslash_path().

The function gracefully handles cases where GetShortPathName() fails (such as when the path doesn't exist or short names are disabled) by simply using the original path. This makes it safe to use on paths that might not yet exist, such as configuration directories specified during installation.

## Parameters / Member Variables
- `path`: The null-terminated string containing the path to be cleaned up in-place

## Dependencies
- Functions called/Symbols referenced:
  - debackslash_path
  - PG_SQL_ASCII
  - GetShortPathName (Windows API)
- Called from (representative examples):
  - get_configdata (multiple calls for different configuration paths)

## Notes and Other Information
- This function only operates on Windows platforms (WIN32 build)
- On non-Windows platforms, this function is effectively a no-op
- The function modifies the input path string in-place
- Uses MAXPGPATH - 1 as the buffer size limit for GetShortPathName()
- Assumes input paths are in server-safe encoding, so uses PG_SQL_ASCII for debackslash_path()
- Designed to create paths compatible with both cmd.exe and MSYS environments
- Particularly useful for paths that may contain spaces, which can cause issues in shell environments
- Safe to use on non-existent paths (will just return the original path if GetShortPathName fails)