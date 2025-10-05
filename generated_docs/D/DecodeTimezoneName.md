# DecodeTimezoneName

## Location
[src/backend/utils/adt/datetime.c:3190-3244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L3190-L3244)

## Overview
DecodeTimezoneName interprets timezone strings as either abbreviations or full timezone names, providing a unified interface for timezone resolution with comprehensive error handling.

## Definition
```c
int DecodeTimezoneName(const char *tzname, int *offset, pg_tz **tz)
```

## Detailed Description
This function serves as the primary entry point for timezone name resolution in PostgreSQL. It implements a two-tier lookup strategy:

1. **Abbreviation lookup**: First attempts to resolve the input as a timezone abbreviation (e.g., "EST", "PST")
2. **Full name lookup**: If abbreviation lookup fails, attempts to resolve as a full timezone database name (e.g., "America/New_York")

The function handles three types of timezone identifiers:
- **TZNAME_FIXED_OFFSET**: Static timezone abbreviations with fixed UTC offsets
- **TZNAME_DYNTZ**: Dynamic timezone abbreviations that reference underlying timezone objects
- **TZNAME_ZONE**: Full timezone database names

The lookup order prioritizes abbreviations over full names to handle cases where the timezone database contains zone names identical to offset abbreviations.

## Parameters / Member Variables
- `tzname`: Input timezone name or abbreviation string
- `offset`: Output UTC offset in seconds (for fixed offset timezones, ISO convention: positive = east)
- `tz`: Output timezone object pointer (for dynamic or full timezone names)

## Dependencies
- Functions called/Symbols referenced:
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md) (string processing function)
  - [DecodeTimezoneAbbrev](DecodeTimezoneAbbrev.md) (timezone abbreviation resolver)
  - [DateTimeParseError](DateTimeParseError.md) (error handling function)
  - [pg_tzset](../p/pg_tzset.md) (timezone database lookup function)
  - ereport, errcode, errmsg (PostgreSQL error reporting)
  - [pg_tz](../p/pg_tz.md) (PostgreSQL timezone type)
  - [DateTimeErrorExtra](DateTimeErrorExtra.md) (error information structure)
  - TZ, DTZ, DYNTZ (timezone type constants)
  - TZNAME_FIXED_OFFSET, TZNAME_DYNTZ, TZNAME_ZONE (return type constants)

- Called from (representative examples):
  - [timetz_zone](../t/timetz_zone.md) (time with timezone conversion functions)
  - [timestamp_zone](../t/timestamp_zone.md), timestamptz_zone (timestamp timezone conversion)
  - [parse_sane_timezone](../p/parse_sane_timezone.md) (timezone validation function)
  - [DecodeTimezoneNameToTz](DecodeTimezoneNameToTz.md) (wrapper function)

## Notes and Other Information
- Returns timezone type constants indicating the kind of identifier found
- Throws PostgreSQL errors for unrecognized timezone names
- Input is automatically converted to lowercase for abbreviation lookup
- Prioritizes abbreviation table over full timezone database for ambiguous names
- Uses comprehensive error reporting with specific error codes and messages
- The function ensures that at least one output parameter (*offset or *tz) is set based on the timezone type
- Fixed offset results use ISO sign convention (positive = east of Greenwich)
- Dynamic timezones require additional resolution through the underlying timezone object

## Simplified Source

```c
int DecodeTimezoneName(const char *tzname, int *offset, pg_tz **tz) {
    int dterr, type;
    DateTimeErrorExtra extra;

    // Convert to lowercase for abbreviation lookup
    char *lowzone = downcase_truncate_identifier(tzname, strlen(tzname), false);

    // First try timezone abbreviation lookup
    dterr = DecodeTimezoneAbbrev(0, lowzone, &type, offset, tz, &extra);
    if (dterr) {
        DateTimeParseError(dterr, &extra, NULL, NULL, NULL);
    }

    if (type == TZ || type == DTZ) {
        // Fixed-offset abbreviation (e.g., "EST", "PST")
        return TZNAME_FIXED_OFFSET;
    } else if (type == DYNTZ) {
        // Dynamic-offset abbreviation (references a timezone)
        return TZNAME_DYNTZ;
    } else {
        // Try as full timezone name (e.g., "America/New_York")
        *tz = pg_tzset(tzname);
        if (*tz == NULL) {
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("time zone \"%s\" not recognized", tzname)));
        }
        return TZNAME_ZONE;
    }
}
```