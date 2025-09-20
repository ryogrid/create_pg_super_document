# TAR_OFFSET_PREFIX

## Location
[src/include/pgtar.h:54-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgtar.h#L54-L57)

## Overview
An enumeration constant that defines the byte offset for the prefix field within a 512-byte tar header structure.

## Definition

```c
enum tarFileType
{
	TAR_FILETYPE_PLAIN = '0',
	TAR_FILETYPE_SYMLINK = '2',
	TAR_FILETYPE_DIRECTORY = '5',
};
```
## Detailed Description
TAR_OFFSET_PREFIX is a member of the tarHeaderOffset enumeration that specifies the byte offset (345) where the prefix field begins within a standard 512-byte tar header block. The prefix field is a 155-byte string field that, according to the POSIX tar standard, can be used to store the leading portion of a file path when the complete path name is too long to fit in the standard 100-byte name field.

This field is part of the POSIX tar format (ustar format) and allows for longer pathnames by concatenating the prefix field with the name field. The tar format uses this mechanism to support file paths longer than 100 characters while maintaining backward compatibility.

## Parameters / Member Variables
This is an enumeration constant representing the offset value 345 with no parameters or member variables.

## Dependencies
- Functions called/Symbols referenced: None (enumeration constant)
- Used by: Currently no direct references found in the codebase, but available for tar header manipulation functions

## Notes and Other Information
- Part of the tarHeaderOffset enumeration which defines field positions within tar headers
- Represents byte offset 345 within a 512-byte tar header block
- The prefix field occupies 155 bytes (from offset 345 to 499)
- Used in POSIX tar format (ustar) for handling long pathnames
- After this field, the last 12 bytes of the 512-byte block remain unassigned
- While not currently referenced in the PostgreSQL codebase, it's defined for completeness of the tar header specification
- Essential for proper tar format compliance and potential future use in path handling