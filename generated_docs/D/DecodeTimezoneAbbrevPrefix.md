# DecodeTimezoneAbbrevPrefix

## Location
[src/backend/utils/adt/datetime.c:3273-3339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L3273-L3339)

## Overview
Interprets a prefix of a string as a timezone abbreviation, matching the longest possible prefix and applying downcasing internally for formatting operations.

## Definition
```c
int DecodeTimezoneAbbrevPrefix(const char *str, int *offset, pg_tz **tz)
```

## Detailed Description
This function provides timezone abbreviation parsing functionality specifically adapted for formatting operations. Unlike DecodeTimezoneAbbrev(), it matches the longest possible prefix of the input string rather than requiring a complete match, and handles downcasing internally. The function searches through successively truncated strings to find the longest valid timezone abbreviation prefix. For fixed-offset abbreviations, it returns the GMT offset; for dynamic abbreviations, it returns the corresponding pg_tz structure.

## Parameters / Member Variables
- `str`: Input string containing potential timezone abbreviation prefix
- `offset`: Output parameter set to GMT offset for fixed-offset abbreviations
- `tz`: Output parameter set to pg_tz structure for dynamic abbreviations

## Dependencies
- Functions called/Symbols referenced:
  - [datebsearch](../d/datebsearch.md)
  - [pg_tolower](../p/pg_tolower.md)
  - [FetchDynamicTimeZone](../F/FetchDynamicTimeZone.md)
  - datetkn
  - TOKMAXLEN
  - DYNTZ
  - [DateTimeErrorExtra](DateTimeErrorExtra.md)
  - [pg_tz](../p/pg_tz.md)
- Called from (representative examples):
  - [DCH_from_char](DCH_from_char.md)

## Notes and Other Information
- Returns the length of the matched timezone abbreviation, or -1 if no match found
- Uses binary search optimization through datebsearch for efficient lookup
- Handles both fixed-offset (numeric) and dynamic (named) timezone abbreviations
- The function requires zoneabbrevtbl to be initialized, returning -1 immediately if not available
- Downcasing is performed internally using pg_tolower for case-insensitive matching
- Declared in src/include/utils/datetime.h with TZNAME_ZONE constant

## Simplified Source

```c
int
DecodeTimezoneAbbrevPrefix(const char *str, int *offset, pg_tz **tz)
{
    char lowtoken[TOKMAXLEN + 1];
    int len;

    // Initialize output parameters
    *offset = 0;
    *tz = NULL;

    // Check if timezone abbreviation table is available
    if (!zoneabbrevtbl)
        return -1;

    // Convert input to lowercase for case-insensitive matching
    for (len = 0; len < TOKMAXLEN; len++) {
        if (*str == '\0' || !isalpha((unsigned char) *str))
            break;
        lowtoken[len] = pg_tolower((unsigned char) *str++);
    }
    lowtoken[len] = '\0';

    // Search with successively shorter strings to find longest match
    while (len > 0) {
        const datetkn *tp = datebsearch(lowtoken, zoneabbrevtbl->abbrevs,
                                       zoneabbrevtbl->numabbrevs);

        if (tp != NULL) {
            if (tp->type == DYNTZ) {
                // Handle dynamic timezone abbreviation
                DateTimeErrorExtra extra;
                pg_tz *tzp = FetchDynamicTimeZone(zoneabbrevtbl, tp, &extra);

                if (tzp != NULL) {
                    *tz = tzp;
                    return len;  // Return length of matched abbreviation
                }
            } else {
                // Handle fixed-offset timezone abbreviation
                *offset = tp->value;
                return len;  // Return length of matched abbreviation
            }
        }

        // Try shorter string by removing last character
        lowtoken[--len] = '\0';
    }

    // No match found
    return -1;
}
```