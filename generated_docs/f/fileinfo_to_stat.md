# fileinfo_to_stat

## Location
src/port/win32stat.c: 68 - 112

## Overview
Converts Windows file information obtained from a file handle to a Unix-style struct stat structure.

## Definition
```c
static int fileinfo_to_stat(HANDLE hFile, struct stat *buf)
```

## Detailed Description
This function bridges the gap between Windows file system APIs and Unix-style file information structures. It uses GetFileInformationByHandle() to retrieve comprehensive file metadata from Windows and translates it into the standard Unix stat structure format that PostgreSQL expects.

The function handles all major stat fields:
- File timestamps (creation, modification, access times) using filetime_to_time conversion
- File mode and permissions using fileattr_to_unixmode conversion  
- File size by combining high and low 32-bit parts into a 64-bit value
- Number of hard links directly from Windows file information

Error handling is provided through _dosmaperr() which maps Windows error codes to Unix errno values.

## Parameters / Member Variables
- `hFile`: Windows file handle obtained from CreateFile() or similar API
- `buf`: Pointer to struct stat that will be populated with file information

## Dependencies
- Functions called/Symbols referenced:
  - GetFileInformationByHandle (Windows API)
  - _dosmaperr (error mapping function)
  - filetime_to_time (timestamp conversion)
  - fileattr_to_unixmode (permission conversion)
  - GetLastError (Windows API)
- Called from:
  - _pglstat64 (at src/port/win32stat.c:143)
  - _pgfstat64 (at src/port/win32stat.c:275)

## Dependencies
- Functions called/Symbols referenced:
  - filetime_to_time
  - fileattr_to_unixmode  
  - _dosmaperr
- Called from (representative examples):
  - _pglstat64
  - _pgfstat64

## Notes and Other Information
- This is a static function, only accessible within the win32stat.c file
- Returns 0 on success, -1 on failure (following Unix stat() convention)
- Handles cases where timestamps might be zero by falling back to modification time
- Combines Windows 64-bit file sizes from separate high/low 32-bit components
- Part of PostgreSQL's Windows compatibility layer for file system operations
- Requires Windows XP or Windows Server 2003 minimum (but this covers all supported platforms)