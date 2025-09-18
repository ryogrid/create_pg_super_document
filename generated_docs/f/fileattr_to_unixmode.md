# fileattr_to_unixmode

## Location
[src/port/win32stat.c:48-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32stat.c#L48-L67)

## Overview
Converts Windows file attributes to a Unix-style file mode, setting only owner permissions.

## Definition
```c
static unsigned short fileattr_to_unixmode(int attr)
```

## Detailed Description
This function maps Windows file attributes to Unix-style file permission bits. It creates a simplified Unix mode that only includes owner permissions, as Windows file attributes don't directly correspond to Unix group and other permissions. The function handles the basic file type (regular file vs directory) and read/write permissions based on the readonly attribute.

The conversion logic:
1. Sets file type bit (_S_IFDIR for directories, _S_IFREG for regular files)
2. Sets read permission for owner (_S_IREAD always set)
3. Sets write permission for owner (_S_IWRITE set unless FILE_ATTRIBUTE_READONLY)
4. Always sets execute permission for owner (_S_IEXEC)

## Parameters / Member Variables
- `attr`: Windows file attributes as an integer bitmask containing FILE_ATTRIBUTE_* flags

## Dependencies
- Functions called/Symbols referenced:
  - FILE_ATTRIBUTE_DIRECTORY (Windows constant)
  - FILE_ATTRIBUTE_READONLY (Windows constant)
  - _S_IFDIR, _S_IFREG, _S_IREAD, _S_IWRITE, _S_IEXEC (Unix mode constants)
- Called from:
  - [fileinfo_to_stat](fileinfo_to_stat.md) (at src/port/win32stat.c:100)

## Notes and Other Information
- This is a static function, only accessible within the win32stat.c file
- Only owner permissions are set, no group or other permissions
- Execute permission is always granted, as noted in the comment about not simulating _S_IEXEC using CMD's PATHEXT extensions
- Part of PostgreSQL's Windows compatibility layer for translating file system metadata