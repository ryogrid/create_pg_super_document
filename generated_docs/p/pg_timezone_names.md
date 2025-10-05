# pg_timezone_names

## Location
[src/backend/utils/adt/datetime.c:5122-5182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L5122-L5182)

## Overview
A set-returning function that reads all available full time zones and returns a set of (name, abbrev, utc_offset, is_dst) for each timezone.

## Definition

```c
enum  *tzenum;
```
## Detailed Description
The  function is a PostgreSQL built-in function that enumerates all available timezone definitions and returns detailed information about each one. It's implemented as a set-returning function (SRF) that materializes its results in a tuplestore.

The function iterates through all available timezones using the timezone enumeration API, converts the current transaction start timestamp to local time in each timezone, and extracts timezone information including the timezone name, abbreviation, UTC offset, and daylight saving time status.

The function includes special handling for problematic timezone abbreviations, particularly rejecting ridiculously long abbreviations (over 31 characters) that were historically produced by IANA's "Factory" timezone or modified by some packagers.

## Parameters / Member Variables
This function takes no explicit parameters but uses the standard PostgreSQL function calling convention:
- Uses  macro for function arguments
- Returns a  (0 for SRF completion)
- Accesses result information via 

## Dependencies
- Functions called/Symbols referenced:
  -  - [Initialize](../I/Initialize.md) materialized set-returning function
  -  - Start timezone enumeration
  -  - Get next timezone in enumeration
  -  - End timezone enumeration
  -  - Get current transaction start time
  -  - Convert timestamp to broken-down time structure
  -  - Get canonical timezone name
  -  - Convert interval structure to Interval datum
  -  - Store tuple values in result set
  -  - Memory initialization utility
  - Various data conversion functions (, , )

- Called from:
  - This function is exposed as a SQL function and called directly from SQL queries, not typically called from other C functions

## Notes and Other Information
- Location: 
- This function is typically exposed to SQL as  system function
- Returns 4 columns: timezone name (text), abbreviation (text), UTC offset (interval), and DST flag (boolean)
- Filters out timezone abbreviations longer than 31 characters to prevent display issues
- Uses the current transaction start timestamp as the reference point for timezone conversions
- The UTC offset is returned as a negative interval (positive values indicate time zones west of UTC)
- Handles conversion failures gracefully by skipping problematic timezones
- Part of PostgreSQL's timezone support infrastructure

## Simplified Source

```c
Datum pg_timezone_names(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    pg_tzenum *tzenum;
    pg_tz *tz;

    InitMaterializedSRF(fcinfo, 0);

    // Start enumerating through all available timezones
    tzenum = pg_tzenumerate_start();

    // Process each timezone
    for (;;) {
        tz = pg_tzenumerate_next(tzenum);
        if (!tz) break;

        // Convert current time to local time in this timezone
        int tzoff;
        struct pg_tm tm;
        fsec_t fsec;
        const char *tzn;

        if (timestamp2tm(GetCurrentTransactionStartTimestamp(),
                        &tzoff, &tm, &fsec, &tzn, tz) != 0)
            continue;  // Skip if conversion fails

        // Filter out ridiculously long abbreviations (> 31 chars)
        // Some versions of IANA "Factory" timezone produce these
        if (tzn && strlen(tzn) > 31)
            continue;

        // Build result tuple with 4 columns
        Datum values[4];
        bool nulls[4] = {0};

        // Column 1: Timezone name
        values[0] = CStringGetTextDatum(pg_get_timezone_name(tz));

        // Column 2: Current abbreviation
        values[1] = CStringGetTextDatum(tzn ? tzn : "");

        // Column 3: UTC offset as interval
        struct pg_itm_in itm_in;
        MemSet(&itm_in, 0, sizeof(struct pg_itm_in));
        itm_in.tm_usec = (int64)-tzoff * USECS_PER_SEC;  // Note: negative for display
        Interval *resInterval = (Interval *)palloc(sizeof(Interval));
        (void)itmin2interval(&itm_in, resInterval);
        values[2] = IntervalPGetDatum(resInterval);

        // Column 4: DST status
        values[3] = BoolGetDatum(tm.tm_isdst > 0);

        // Add tuple to result set
        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

    pg_tzenumerate_end(tzenum);
    return (Datum)0;
}
```