# TAR_SYMLINK_TOO_LONG

## Location
[src/include/pgtar.h:23-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgtar.h#L23-L36)

## Overview
An enumeration constant representing an error condition when a symbolic link target path exceeds the maximum length allowed in tar archive format.

## Definition

```c
enum tarHeaderOffset
{
	TAR_OFFSET_NAME = 0,		/* 100 byte string */
	TAR_OFFSET_MODE = 100,		/* 8 byte tar number, excludes S_IFMT */
	TAR_OFFSET_UID = 108,		/* 8 byte tar number */
	TAR_OFFSET_GID = 116,		/* 8 byte tar number */
	TAR_OFFSET_SIZE = 124,		/* 8 byte tar number */
	TAR_OFFSET_MTIME = 136,		/* 12 byte tar number */
	TAR_OFFSET_CHECKSUM = 148,	/* 8 byte tar number */
	TAR_OFFSET_TYPEFLAG = 156,	/* 1 byte file type, see TAR_FILETYPE_* */
	TAR_OFFSET_LINKNAME = 157,	/* 100 byte string */
	TAR_OFFSET_MAGIC = 257,		/* "ustar" with terminating zero byte */
	TAR_OFFSET_VERSION = 263,	/* "00" */
	TAR_OFFSET_UNAME = 265,		/* 32 byte string */
	TAR_OFFSET_GNAME = 297,		/* 32 byte string */
	TAR_OFFSET_DEVMAJOR = 329,	/* 8 byte tar number */
	TAR_OFFSET_DEVMINOR = 337,	/* 8 byte tar number */
	TAR_OFFSET_PREFIX = 345,	/* 155 byte string */
	/* last 12 bytes of the 512-byte block are unassigned */
};
```
## Detailed Description
TAR_SYMLINK_TOO_LONG is a member of the tarError enumeration that indicates an error condition when attempting to create a tar archive entry for a symbolic link whose target path is too long to fit within the tar format constraints. The tar format has specific limitations on the length of symbolic link targets, and this error code is returned when those limits are exceeded.

This error helps ensure that tar archives created by PostgreSQL comply with standard tar format specifications and can be properly read by standard tar utilities.

## Parameters / Member Variables
This is an enumeration constant with no parameters or member variables.

## Dependencies
- Functions called/Symbols referenced: None (enumeration constant)
- Used by (representative examples):
  - [_tarWriteHeader](../t/_tarWriteHeader.md) (src/backend/backup/basebackup.c:2051)
  - tarCreateHeader (src/port/tar.c:121)

## Notes and Other Information
- Part of the tarError enumeration which provides standardized error codes for tar operations in PostgreSQL
- Used in conjunction with TAR_OK and TAR_NAME_TOO_LONG to provide comprehensive error handling for tar archive creation
- Helps maintain compatibility with standard tar format specifications
- Essential for proper error reporting during base backup operations and other tar-related functionality