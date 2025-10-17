# validateTzEntry

## Location
[src/backend/utils/misc/tzparser.c:52-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/tzparser.c#L52-L97)

## Overview
Applies additional validation checks to a timezone entry structure to ensure it meets PostgreSQL's requirements for timezone abbreviations.

## Definition
static bool validateTzEntry(tzEntry *tzentry)

## Detailed Description
This function performs several validation checks on a timezone entry before it can be added to PostgreSQL's timezone abbreviation table. It enforces restrictions imposed by the datetktbl storage format and performs sanity checks on the timezone data. The function converts the abbreviation to lowercase to match datetime.c's expected format.

## Parameters / Member Variables
- : Pointer to a tzEntry structure containing timezone abbreviation data to be validated

## Dependencies
- Functions called/Symbols referenced:
  - TOKMAXLEN
  - GUC_check_errmsg
  - SECS_PER_HOUR 
  - [pg_tolower](../p/pg_tolower.md)
- Called from (representative examples):
  - [ParseTzFile](../P/ParseTzFile.md)

## Notes and Other Information
The function enforces a maximum length limit for timezone abbreviations (TOKMAXLEN characters) and validates that timezone offsets don't exceed ±14 hours, which is a reasonable sanity check for valid timezone offsets. All abbreviations are converted to lowercase for consistency with PostgreSQL's internal datetime handling.

## Simplified Source

```c
static bool validateTzEntry(tzEntry *tzentry) {
    // Check abbreviation length limit
    if (strlen(tzentry->abbrev) > TOKMAXLEN) {
        GUC_check_errmsg("time zone abbreviation \"%s\" is too long (maximum %d characters)",
                         tzentry->abbrev, TOKMAXLEN);
        return false;
    }

    // Validate offset is within ±14 hours
    if (tzentry->offset > 14 * SECS_PER_HOUR ||
        tzentry->offset < -14 * SECS_PER_HOUR) {
        GUC_check_errmsg("time zone offset %d is out of range", tzentry->offset);
        return false;
    }

    // Convert abbreviation to lowercase for consistency
    for (unsigned char *p = (unsigned char *) tzentry->abbrev; *p; p++) {
        *p = pg_tolower(*p);
    }

    return true;
}
```