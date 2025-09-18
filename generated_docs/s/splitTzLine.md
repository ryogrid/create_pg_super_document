# splitTzLine

## Location
[src/backend/utils/misc/tzparser.c:98-187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/tzparser.c#L98-L187)

## Overview
Parses a single line from a timezone abbreviation file and extracts timezone information into a tzEntry structure.

## Definition
static bool splitTzLine(const char *filename, int lineno, char *line, tzEntry *tzentry)

## Detailed Description
This function attempts to parse a line from a timezone abbreviation file according to two valid formats: 'name zone' for zone names, or 'name offset dst' for numeric offsets. It handles both timezone names (which reference external timezone definitions) and direct numeric offsets with optional daylight saving time flags. The function performs input validation and populates the tzentry structure with the parsed data.

## Parameters / Member Variables
- : Name of the timezone file being processed (for error reporting)
- : Current line number in the file (for error reporting)
- : The line of text to parse
- : Pointer to tzEntry structure to populate with parsed data

## Dependencies
- Functions called/Symbols referenced:
  - strtok
  - WHITESPACE
  - GUC_check_errmsg
  - [pstrdup](../p/pstrdup.md)
  - isdigit
  - strtol
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - SECS_PER_HOUR
- Called from (representative examples):
  - [ParseTzFile](../P/ParseTzFile.md)

## Notes and Other Information
The function distinguishes between zone names and numeric offsets by checking if the second token begins with a digit or sign. Zone names are assumed to be valid without validation to avoid loading unnecessary timezone data. Comments beginning with '#' are allowed and ignored after the main timezone specification.