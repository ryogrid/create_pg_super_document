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

## Simplified Source

```c
static bool
splitTzLine(const char *filename, int lineno, char *line, tzEntry *tzentry)
{
    char *abbrev, *offset, *is_dst, *remain;

    // Initialize entry metadata
    tzentry->lineno = lineno;
    tzentry->filename = filename;

    // Parse abbreviation (first token)
    abbrev = strtok(line, WHITESPACE);
    if (!abbrev) {
        GUC_check_errmsg("missing time zone abbreviation");
        return false;
    }
    tzentry->abbrev = pstrdup(abbrev);

    // Parse offset/zone (second token)
    offset = strtok(NULL, WHITESPACE);
    if (!offset) {
        GUC_check_errmsg("missing time zone offset");
        return false;
    }

    // Check if it's a numeric offset or zone name
    if (isdigit(*offset) || *offset == '+' || *offset == '-') {
        // Numeric offset format
        tzentry->zone = NULL;
        tzentry->offset = strtol(offset, NULL, 10);

        // Check for optional DST flag
        is_dst = strtok(NULL, WHITESPACE);
        if (is_dst && pg_strcasecmp(is_dst, "D") == 0) {
            tzentry->is_dst = true;
            remain = strtok(NULL, WHITESPACE);
        } else {
            tzentry->is_dst = false;
            remain = is_dst;
        }
    } else {
        // Zone name format
        tzentry->zone = pstrdup(offset);
        tzentry->offset = 0;
        tzentry->is_dst = false;
        remain = strtok(NULL, WHITESPACE);
    }

    // Check for comments (must start with #)
    if (remain && remain[0] != '#') {
        GUC_check_errmsg("invalid syntax");
        return false;
    }

    return true;
}
```