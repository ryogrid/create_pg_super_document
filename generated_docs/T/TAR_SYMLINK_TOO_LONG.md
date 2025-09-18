# TAR_SYMLINK_TOO_LONG

## Location
src/include/pgtar.h: 23 - 36

## Overview
An enumeration constant representing an error condition when a symbolic link target path exceeds the maximum length allowed in tar archive format.

## Definition


## Detailed Description
TAR_SYMLINK_TOO_LONG is a member of the tarError enumeration that indicates an error condition when attempting to create a tar archive entry for a symbolic link whose target path is too long to fit within the tar format constraints. The tar format has specific limitations on the length of symbolic link targets, and this error code is returned when those limits are exceeded.

This error helps ensure that tar archives created by PostgreSQL comply with standard tar format specifications and can be properly read by standard tar utilities.

## Parameters / Member Variables
This is an enumeration constant with no parameters or member variables.

## Dependencies
- Functions called/Symbols referenced: None (enumeration constant)
- Used by (representative examples):
  - _tarWriteHeader (src/backend/backup/basebackup.c:2051)
  - tarCreateHeader (src/port/tar.c:121)

## Notes and Other Information
- Part of the tarError enumeration which provides standardized error codes for tar operations in PostgreSQL
- Used in conjunction with TAR_OK and TAR_NAME_TOO_LONG to provide comprehensive error handling for tar archive creation
- Helps maintain compatibility with standard tar format specifications
- Essential for proper error reporting during base backup operations and other tar-related functionality