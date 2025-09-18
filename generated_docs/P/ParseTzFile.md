# ParseTzFile

## Location
[src/backend/utils/misc/tzparser.c:276-446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/tzparser.c#L276-L446)

## Overview
Parses a single timezone abbreviation file with support for recursion to handle @INCLUDE directives and builds an array of timezone entries.

## Definition
static int ParseTzFile(const char *filename, int depth, tzEntry **base, int *arraysize, int n)

## Detailed Description
This function is the core parser for PostgreSQL's timezone abbreviation files. It reads and processes timezone files line by line, supporting special directives like @INCLUDE for file inclusion and @OVERRIDE for duplicate handling. The function enforces security restrictions on filenames, manages recursion depth, and coordinates with helper functions to parse individual lines and maintain the sorted timezone array. It handles file I/O errors gracefully and provides detailed error reporting.

## Parameters / Member Variables
- : User-specified timezone file name (without path, must be alpha characters only)
- : Current recursion depth (limited to 3 levels)
- : Array for results (changeable if array must be enlarged)
- : Allocated length of array (changeable if array must be enlarged)
- : Current number of valid elements in array

## Dependencies
- Functions called/Symbols referenced:
  - isalpha
  - GUC_check_errmsg
  - [get_share_path](../g/get_share_path.md)
  - snprintf
  - AllocateFile
  - AllocateDir
  - FreeDir
  - feof
  - fgets
  - ferror
  - strlen
  - isspace
  - [pg_strncasecmp](../p/pg_strncasecmp.md)
  - [pstrdup](../p/pstrdup.md)
  - strtok
  - WHITESPACE
  - [splitTzLine](../s/splitTzLine.md)
  - [validateTzEntry](../v/validateTzEntry.md)
  - [addToArray](../a/addToArray.md)
  - FreeFile
- Called from (representative examples):
  - [ParseTzFile](ParseTzFile.md) (recursive calls)
  - [load_tzoffsets](../l/load_tzoffsets.md)

## Notes and Other Information
The function enforces several security and sanity checks: filenames must contain only alphabetic characters, recursion is limited to 3 levels, and lines cannot exceed the buffer size. Special directives @INCLUDE and @OVERRIDE provide flexibility in organizing timezone data across multiple files. The parser automatically skips empty lines and comments (lines beginning with #).