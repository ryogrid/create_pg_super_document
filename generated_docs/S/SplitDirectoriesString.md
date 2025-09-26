# SplitDirectoriesString

## Location
[src/backend/utils/adt/varlena.c:3584-3704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3584-L3704)

## Overview
Parses a string containing file/directory paths separated by a delimiter, handling pathname-specific parsing rules including path canonicalization and embedded spaces.

## Definition

```c
bool
SplitDirectoriesString(char *rawstring, char separator,
					   List **namelist)
```
## Detailed Description
The  function is a specialized parsing utility designed for processing lists of file and directory paths. Unlike , this function is tailored for filesystem paths rather than SQL identifiers, implementing different parsing rules appropriate for pathnames.

Key differences from identifier parsing:
- **No Case Conversion**: Preserves the original case of path components since filesystems may be case-sensitive
- **Embedded Spaces**: Allows spaces within unquoted paths, with intelligent trailing whitespace removal
- **Path Length Limits**: Uses  as the maximum length instead of identifier length limits
- **Path Canonicalization**: Applies  to normalize path separators and resolve relative components
- **Memory Allocation**: Returns separately allocated strings rather than pointers into the input string due to canonicalization

The function handles both quoted and unquoted paths:
- Quoted paths support quote-quote escape sequences and can contain any characters
- Unquoted paths extend to the separator or string end, with trailing whitespace trimmed
- Empty strings are allowed, but empty unquoted path components are not

## Parameters / Member Variables
- : Input string to be parsed (must be modifiable; will be overwritten during parsing)
- : Character used to separate paths (typically ',' or ';' depending on platform conventions)
- : Output parameter filled with a list of separately allocated, canonicalized path strings

## Dependencies
- Functions called/Symbols referenced:
  - : Checks for whitespace characters using PostgreSQL's scanner rules
  - : Finds quote characters in quoted paths
  - : Collapses quote-quote escape sequences
  - : String length calculation and path length checking
  - : Allocates separate copy of each parsed path
  - : Normalizes path format and resolves relative components
  - : Adds parsed path to result list
  - : Maximum path length constant

- Called from (representative examples):
  - : Parsing library path configuration during server startup
  - : Processing shared_preload_libraries and other library path settings

## Notes and Other Information
- Despite the name, this function works equally well for individual file names and directory paths
- The function name is historical; it was originally designed for directory lists but expanded to handle general paths
- Critical for PostgreSQL's library loading mechanism and path configuration processing
- Each returned path string is separately allocated and canonicalized, requiring  for cleanup
- Path canonicalization ensures consistent path representation across different platforms
- Supports platform-specific path separators through the canonicalization process
- Maximum path length enforcement prevents buffer overflows in downstream path processing
- Used primarily during server initialization and configuration processing
- Location: src/backend/utils/adt/varlena.c:3584-3704