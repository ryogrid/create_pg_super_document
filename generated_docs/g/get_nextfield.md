# get_nextfield

## Location
src/backend/tsearch/spell.c: 792 - 857

## Overview
Parses the next space-separated field from an .affix file line, handling multibyte characters and comment detection.

## Definition
```c
static bool get_nextfield(char **str, char *next)
```

## Detailed Description
get_nextfield implements a state machine to extract whitespace-separated fields from affix file lines. The function operates in two states:

1. **PAE_WAIT_MASK**: Waiting for the start of a field, skipping whitespace
2. **PAE_INMASK**: Reading characters within a field until whitespace is encountered

The parser handles several important features:
- Multibyte character support using pg_mblen() and COPYCHAR()
- Comment detection (lines starting with '#' are ignored)
- Buffer overflow prevention by limiting output to BUFSIZ
- Proper null termination of extracted fields

The function advances the input pointer past the parsed field, allowing successive calls to extract multiple fields from the same line.

## Parameters / Member Variables
- `str`: Pointer to input string pointer (modified to advance past the parsed field)
- `next`: Output buffer where the extracted field will be copied (must be BUFSIZ in size)

## Dependencies
- Functions called/Symbols referenced:
  - t_iseq
  - [t_isspace](../t/t_isspace.md)
  - [pg_mblen](../p/pg_mblen.md)
  - COPYCHAR
  - PAE_WAIT_MASK
  - PAE_INMASK
- Called from (representative examples):
  - [parse_ooaffentry](../p/parse_ooaffentry.md)

## Notes and Other Information
- Returns true if a field was successfully extracted, false if end of line or comment encountered
- Static function, only accessible within the spell.c module
- Uses a state machine approach for robust parsing
- Truncates fields that exceed BUFSIZ to prevent buffer overflow
- Handles multibyte characters correctly for international character sets
- Comment lines (starting with '#') cause immediate return with false
- Preserves the integrity of multibyte character sequences during copying