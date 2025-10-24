# doabbr

## Location
[src/timezone/zic.c:2630-2672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2630-L2672)

## Overview
The doabbr function generates timezone abbreviations from zone format strings, handling various formatting patterns and special cases like offset-based abbreviations and quoted non-alphabetic abbreviations.

## Definition

```c
static size_t
doabbr(char *abbr, struct zone const *zp, char const *letters,
	   bool isdst, zic_t save, bool doquotes)
```
## Detailed Description
This function creates timezone abbreviations based on the zone's format specification. It handles several different abbreviation formats:

1. **Simple format strings**: Uses sprintf to substitute letters into format patterns
2. **Slash-separated formats**: For zones with different standard/daylight abbreviations (e.g., "EST/EDT")
3. **Offset-based abbreviations**: When format specifier is 'z', generates numeric offset abbreviations
4. **Quoted abbreviations**: Adds angle brackets around non-alphabetic abbreviations for compatibility

The function intelligently handles the daylight saving time context, selecting appropriate parts of slash-separated formats and calculating correct offsets for numeric abbreviations.

## Parameters / Member Variables
- `*abbr`: Output buffer where the generated abbreviation will be written
- `*zp`: Pointer to the zone structure containing format information
- `*letters`: Variable part of the abbreviation (e.g., "D" in "EDT")
- `isdst`: Boolean indicating if this is for daylight saving time
- `save`: Amount of time saved during daylight saving time
- `doquotes`: Whether to add angle brackets around non-alphabetic abbreviations
## Dependencies
- Functions called/Symbols referenced:
  - strchr (to find slash separator in format)
  - [abbroffset](../a/abbroffset.md) (to generate numeric offset strings)
  - sprintf (for format string substitution)
  - strcpy, memcpy (for string copying)
  - strlen (for string length calculation)
  - [is_alpha](../i/is_alpha.md) (to check if abbreviation is alphabetic)
  - memmove (for string manipulation when adding quotes)
- Called from (representative examples):
  - [stringzone](../s/stringzone.md) (in src/timezone/zic.c:2900, 2910)
  - [years_of_observations](../y/years_of_observations.md) (in src/timezone/zic.c:3117, 3232, 3243, 3253)

## Notes and Other Information
- Returns the length of the generated abbreviation
- Handles both standard and daylight saving time abbreviations through slash notation
- Automatically quotes non-alphabetic abbreviations with angle brackets when doquotes is true
- Supports the '%z' format specifier for generating offset-based abbreviations
- Used extensively in timezone processing to generate human-readable timezone names
- The function ensures proper null termination of all generated strings

## Simplified Source

```c
static size_t doabbr(char *abbr, struct zone const *zp, char const *letters,
                     bool isdst, zic_t save, bool doquotes) {
    char *slashp;
    size_t len;
    char const *format = zp->z_format;

    // Check if format contains slash (e.g., "EST/EDT")
    slashp = strchr(format, '/');

    if (slashp == NULL) {
        // Simple format - substitute letters
        if (zp->z_format_specifier == 'z') {
            // Generate numeric offset abbreviation
            char letterbuf[PERCENT_Z_LEN_BOUND + 1];
            letters = abbroffset(letterbuf, zp->z_stdoff + save);
        } else if (!letters) {
            letters = "%s";
        }
        sprintf(abbr, format, letters);
    } else {
        // Slash-separated format - choose part based on DST
        if (isdst) {
            strcpy(abbr, slashp + 1);  // Use part after slash for DST
        } else {
            // Use part before slash for standard time
            memcpy(abbr, format, slashp - format);
            abbr[slashp - format] = '\0';
        }
    }

    len = strlen(abbr);

    // Add angle brackets if needed for non-alphabetic abbreviations
    if (doquotes) {
        char *cp;
        for (cp = abbr; is_alpha(*cp); cp++)
            continue;

        if (len > 0 && *cp != '\0') {
            // Non-alphabetic content found, add quotes
            memmove(abbr + 1, abbr, len);
            abbr[0] = '<';
            abbr[len + 1] = '>';
            abbr[len + 2] = '\0';
            return len + 2;
        }
    }

    return len;
}
```