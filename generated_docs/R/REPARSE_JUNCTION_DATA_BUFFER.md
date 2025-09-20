# REPARSE_JUNCTION_DATA_BUFFER

## Location
[src/port/dirmod.c:207-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/dirmod.c#L207-L208)

## Overview
A Windows-specific structure used to handle reparse point data for junction points and symbolic links on Windows file systems.

## Definition

```c
WCHAR		PathBuffer[FLEXIBLE_ARRAY_MEMBER];
} REPARSE_JUNCTION_DATA_BUFFER;

#define REPARSE_JUNCTION_DATA_BUFFER_HEADER_SIZE   \
		FIELD_OFFSET(REPARSE_JUNCTION_DATA_BUFFER, SubstituteNameOffset)


/*
 *	pgsymlink - uses Win32 junction points
 *
 *	For reference:	http://www.codeproject.com/KB/winsdk/junctionpoints.aspx
 */
int
pgsymlink(const char *oldpath, const char *newpath)
```
## Detailed Description
This structure is a PostgreSQL-specific replacement for the Windows API REPARSE_DATA_BUFFER structure, which was defined in VC6 winnt.h but omitted in later SDK versions. It is used exclusively on Windows (non-Cygwin) platforms to implement symbolic link functionality through Windows junction points. The structure contains the necessary fields from the SymbolicLinkReparseBuffer part of the original union structure, providing a portable way to handle Windows reparse points for PostgreSQL's symlink operations.

## Parameters / Member Variables
- : A DWORD value identifying the type of reparse point
- : Length in bytes of the reparse data following the header
- : Reserved field for alignment and future use
- : Byte offset to the substitute name string within PathBuffer
- : Length in bytes of the substitute name string
- : Byte offset to the print name string within PathBuffer  
- : Length in bytes of the print name string
- : Variable-length buffer containing the substitute and print name strings in WCHAR format

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro)
  - Windows API types: DWORD, WORD, WCHAR
- Called from (representative examples):
  - [pgsymlink](../p/pgsymlink.md) (src/port/dirmod.c:223, 226)
  - [pgreadlink](../p/pgreadlink.md) (src/port/dirmod.c:313, 314)
  - REPARSE_JUNCTION_DATA_BUFFER_HEADER_SIZE (macro definition)

## Notes and Other Information
- Only available on Windows platforms (excluding Cygwin which has its own symlink implementation)
- Located in src/port/dirmod.c:196-207
- Used in conjunction with Windows CreateFile, DeviceIoControl APIs for junction point operations
- The PathBuffer field uses a flexible array member to accommodate variable-length path strings
- Essential for PostgreSQL's cross-platform symlink functionality on Windows systems