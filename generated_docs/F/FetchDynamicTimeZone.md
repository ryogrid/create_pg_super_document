# FetchDynamicTimeZone

## Location
src/backend/utils/adt/datetime.c: 4970 - 5000

## Overview
FetchDynamicTimeZone resolves dynamic timezone abbreviations by lazy-loading the underlying pg_tz timezone object from the system timezone database.

## Definition


## Detailed Description
This helper function handles the lazy resolution of dynamic timezone abbreviations. Dynamic abbreviations reference timezone names (like "America/New_York") rather than fixed UTC offsets, requiring runtime lookup of the actual timezone data.

The function uses a lazy loading strategy:
1. **First access**: If dtza->tz is NULL, calls pg_tzset() to load the timezone from the system database
2. **Subsequent access**: Returns the cached pg_tz pointer directly
3. **Error handling**: If timezone loading fails, populates error context information but doesn't raise an error immediately

This design minimizes startup time by only loading timezone data when actually needed, while providing proper error context for debugging timezone configuration issues.

The function includes safety assertions to prevent out-of-bounds memory access when following the offset pointer from the datetkn to the DynamicZoneAbbrev structure.

## Parameters / Member Variables
- .if !\n(.g .ab GNU tbl requires GNU troff.
.if !dTS .ds TS
.if !dTE .ds TE
.lf 1 -: The TimeZoneAbbrevTable containing the dynamic timezone abbreviation
- : Pointer to the datetkn entry representing the dynamic abbreviation (must have type DYNTZ)
- : Structure to populate with error details if timezone resolution fails

## Dependencies
- Functions called/Symbols referenced:
  - pg_tzset (load timezone from system database)
- Data structures referenced:
  - DynamicZoneAbbrev (dynamic timezone abbreviation structure)
  - datetkn (timezone token structure)
  - TimeZoneAbbrevTable (timezone abbreviation table)
  - DateTimeErrorExtra (error context structure)
- Called from (representative examples):
  - DecodeTimezoneAbbrev (src/backend/utils/adt/datetime.c:3120)
  - DecodeTimezoneAbbrevPrefix (src/backend/utils/adt/datetime.c:3310)
  - pg_timezone_abbrevs (src/backend/utils/adt/datetime.c:5071)

## Notes and Other Information
- This is a static helper function, only accessible within datetime.c
- Uses lazy loading to avoid loading all timezone data at startup
- The function doesn't throw errors itself - callers are responsible for checking the return value and handling DTERR_BAD_ZONE_ABBREV errors
- Memory safety is ensured through offset validation assertions
- The cached pg_tz pointer remains valid for the lifetime of the timezone abbreviation table
- Timezone names come from configuration files and may reference invalid zones if misconfigured