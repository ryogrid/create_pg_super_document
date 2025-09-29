# DetermineTimeZoneAbbrevOffset

## Location
[src/backend/utils/adt/datetime.c:1746-1783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L1746-L1783)

## Overview
Determines the GMT offset and DST flag to be attributed to a dynamic time zone abbreviation whose meaning has changed over time, forcing use of abbreviation-specific offset even when the zone was in different time status.

## Definition
```c
int DetermineTimeZoneAbbrevOffset(struct pg_tm *tm, const char *abbr, pg_tz *tzp)
```

## Detailed Description
This function determines the appropriate GMT offset and daylight saving time (DST) flag for a dynamic time zone abbreviation at a specific point in time. Unlike `DetermineTimeZoneOffset()`, this function forces the use of the abbreviation-specific GMT offset even when the zone was in a different time status (standard vs. daylight time).

The function works by:
1. Computing the UTC time to probe using `DetermineTimeZoneOffsetInternal()`
2. Attempting to match the abbreviation to timezone data using `DetermineTimeZoneAbbrevOffsetInternal()`
3. If a match is found, using the abbreviation-specific offset and DST flag
4. If no match is found, falling back to the standard zone offset

This behavior is crucial for handling historical timezone abbreviations that may have different meanings at different times.

## Parameters / Member Variables
- `tm`: Pointer to pg_tm structure containing the local time for determination; tm_isdst field receives the DST flag
- `abbr`: The timezone abbreviation string to match against IANA timezone data
- `tzp`: Pointer to the timezone definition structure

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tm](../p/pg_tm.md) (struct)
  - [pg_tz](../p/pg_tz.md) (struct) 
  - pg_time_t (type)
  - [DetermineTimeZoneOffsetInternal](DetermineTimeZoneOffsetInternal.md)
  - [DetermineTimeZoneAbbrevOffsetInternal](DetermineTimeZoneAbbrevOffsetInternal.md)
- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md)
  - [DecodeTimeOnly](DecodeTimeOnly.md)
  - [do_to_timestamp](../d/do_to_timestamp.md)
  - [parse_sane_timezone](../p/parse_sane_timezone.md)
  - [timestamp_zone](../t/timestamp_zone.md)

## Notes and Other Information
- This function specifically handles dynamic timezone abbreviations whose meanings have changed over historical time periods
- Falls back to standard timezone offset determination if abbreviation matching fails
- The tm_isdst field in the input structure is modified to reflect the determined DST status
- Handles potential overflow in UTC time computation by probing at the epoch as a fallback
- Located in src/backend/utils/adt/datetime.c:1746-1783

## Simplified Source

```c
int
DetermineTimeZoneAbbrevOffset(struct pg_tm *tm, const char *abbr, pg_tz *tzp)
{
    pg_time_t t;
    int zone_offset;
    int abbr_offset;
    int abbr_isdst;

    // First compute the UTC time to probe at
    // (Falls back to epoch if overflow occurs)
    zone_offset = DetermineTimeZoneOffsetInternal(tm, tzp, &t);

    // Try to match the abbreviation to specific timezone data
    if (DetermineTimeZoneAbbrevOffsetInternal(t, abbr, tzp,
                                             &abbr_offset, &abbr_isdst)) {
        // Found abbreviation-specific match: use its values
        tm->tm_isdst = abbr_isdst;
        return abbr_offset;
    }

    // No abbreviation match found: use standard zone offset
    return zone_offset;
}
```