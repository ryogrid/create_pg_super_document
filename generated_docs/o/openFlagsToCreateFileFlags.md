# openFlagsToCreateFileFlags

## Location
[src/port/open.c:29-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/open.c#L29-L64)

## Overview
Converts POSIX open() flags to Windows CreateFile() disposition flags to determine how a file should be created or opened.

## Definition
```c
static int openFlagsToCreateFileFlags(int openFlags)
```

## Detailed Description
This internal function translates standard POSIX file open flags (O_CREAT, O_TRUNC, O_EXCL) into the appropriate Windows CreateFile() disposition parameter. The function handles all valid combinations of these three flags and maps them to Windows-specific file creation behaviors:

- OPEN_EXISTING: Opens existing files only
- OPEN_ALWAYS: Opens existing files or creates new ones  
- TRUNCATE_EXISTING: Opens and truncates existing files
- CREATE_ALWAYS: Always creates new files (overwrites existing)
- CREATE_NEW: Creates new files only (fails if file exists)

The function uses a switch statement to handle all possible combinations of the three relevant flags, with comments noting when certain flag combinations are meaningless.

## Parameters / Member Variables
- `openFlags`: Integer containing POSIX open flags, specifically examining O_CREAT, O_TRUNC, and O_EXCL bits

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only standard flag constants)
- Called from (representative examples):
  - [pgwin32_open_handle](../p/pgwin32_open_handle.md)

## Notes and Other Information
- This is a static function internal to src/port/open.c
- The function handles Windows-specific file creation semantics that differ from POSIX
- Comments in the code note when certain flag combinations like O_EXCL without O_CREAT are meaningless
- Returns 0 as a fallback case that should never be reached according to the comment