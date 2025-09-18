# getNextFlagFromString

## Location
[src/backend/tsearch/spell.c:349-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L349-L454)

## Overview
A static function that parses and extracts individual affix flags from a string containing multiple flags, supporting different flag representation formats.

## Definition
```c
static void getNextFlagFromString(IspellDict *Conf, char **sflagset, char *sflag)
```

## Detailed Description
The `getNextFlagFromString` function is a crucial component of the PostgreSQL spell-checking system that handles the parsing of affix flags from compressed flag strings. It supports three different flag modes:

1. **FM_CHAR**: Single character flags (e.g., "ABCD" represents flags A, B, C, D)
2. **FM_LONG**: Two-character flags (e.g., "ABCDE*" represents flags AB, CD, E*)  
3. **FM_NUM**: Numeric flags separated by commas (e.g., "200,205,50" represents flags 200, 205, 50)

The function advances through the input string, extracting one flag at a time while validating the format according to the current flag mode. For numeric flags, it performs range checking (0 to FLAGNUM_MAXSIZE) and validates comma separation. It handles Unicode characters properly using PostgreSQL's multi-byte character functions.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure containing dictionary configuration including flagMode
- `sflagset`: Pointer to string pointer that gets advanced to the next flag position
- `sflag`: Output buffer where the extracted flag is stored as a null-terminated string

## Dependencies
- Functions called/Symbols referenced:
  - IspellDict (structure type)
  - FM_LONG, FM_CHAR, FM_NUM (flag mode constants)
  - COPYCHAR (macro for copying characters)
  - [pg_mblen](../p/pg_mblen.md) (PostgreSQL multi-byte length function)
  - FLAGNUM_MAXSIZE (maximum flag number constant)
  - [t_isdigit](../t/t_isdigit.md), t_iseq, t_isspace (text processing functions)
  - strtol (standard C library function)
  - sprintf (standard C library function)
  - ereport, elog (PostgreSQL error reporting functions)
- Called from (representative examples):
  - [IsAffixFlagInUse](../I/IsAffixFlagInUse.md)
  - [getCompoundAffixFlagValue](getCompoundAffixFlagValue.md)

## Notes and Other Information
- Modifies the sflagset pointer to advance to the next flag in the string
- Performs extensive error checking for malformed flag strings
- Handles Unicode characters properly through PostgreSQL's character handling functions
- The function does not return a value but updates the sflag output buffer
- Located in src/backend/tsearch/spell.c:349-454