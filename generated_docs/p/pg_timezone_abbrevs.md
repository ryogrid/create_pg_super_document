# pg_timezone_abbrevs

## Location
[src/backend/utils/adt/datetime.c:5001-5121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L5001-L5121)

## Overview
pg_timezone_abbrevs is a set-returning SQL function that provides access to all available timezone abbreviations in the current PostgreSQL configuration, returning their names, UTC offsets, and daylight saving time status.

## Definition

```c
struct pg_itm_in itm_in;
```
## Detailed Description
This SQL-accessible function exposes PostgreSQL's internal timezone abbreviation table as a queryable result set. It returns three columns for each timezone abbreviation:
- **abbrev**: The timezone abbreviation name (uppercased)
- **utc_offset**: Time offset from UTC as an interval 
- **is_dst**: Boolean indicating if this represents daylight saving time

The function handles three types of timezone entries:
1. **TZ (Static timezone)**: Fixed UTC offset, not daylight saving time
2. **DTZ (Daylight timezone)**: Fixed UTC offset, represents daylight saving time
3. **DYNTZ (Dynamic timezone)**: Variable offset determined by timezone rules at current timestamp

For dynamic timezones, the function performs real-time resolution using the current transaction timestamp to determine the appropriate offset and DST status. This ensures the returned information reflects current timezone rules rather than historical or future states.

The function uses PostgreSQL's Set-Returning Function (SRF) framework to iterate through all entries in the active timezone abbreviation table, maintaining state between calls through the function context.

## Parameters / Member Variables
- Returns: Set of records with (text abbrev, interval utc_offset, boolean is_dst)

## Dependencies
- Functions called/Symbols referenced:
  - SRF framework macros (SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, etc.)
  - [FetchDynamicTimeZone](../F/FetchDynamicTimeZone.md) (resolve dynamic timezone abbreviations)
  - [GetCurrentTransactionStartTimestamp](../G/GetCurrentTransactionStartTimestamp.md) (get current time for dynamic resolution)
  - [DetermineTimeZoneAbbrevOffsetTS](../D/DetermineTimeZoneAbbrevOffsetTS.md) (calculate timezone offset at specific time)
  - [DateTimeParseError](../D/DateTimeParseError.md) (report timezone resolution errors)
  - Memory management (palloc, MemoryContextSwitchTo)
  - [String](../S/String.md)/data conversion utilities (strlcpy, pg_toupper, itmin2interval)
- Data structures referenced:
  - [TimeZoneAbbrevTable](../T/TimeZoneAbbrevTable.md) (via global zoneabbrevtbl)
  - [FuncCallContext](../F/FuncCallContext.md) (SRF state management)
  - datetkn (timezone token entries)
  - [pg_itm_in](pg_itm_in.md), Interval (time interval representation)
- Called from:
  - SQL queries via function call mechanism

## Notes and Other Information
- This function provides the underlying data for the pg_timezone_abbrevs view
- Abbreviation names are converted to uppercase to match PostgreSQL's internal representation
- Dynamic timezone resolution uses the current transaction start time to ensure consistency within a transaction
- The function will raise an error if dynamic timezone resolution fails (e.g., invalid timezone name in configuration)
- Results reflect the currently active timezone_abbreviations configuration setting
- Memory allocated during function execution is automatically cleaned up by the SRF framework
- Returns empty result set if no timezone abbreviation table is loaded

## Simplified Source

```c
Datum pg_timezone_abbrevs(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    int *pindex;

    // First call setup
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Initialize index counter
        pindex = (int *)palloc(sizeof(int));
        *pindex = 0;
        funcctx->user_fctx = (void *)pindex;

        // Set up return tuple descriptor
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            elog(ERROR, "return type must be a row type");
        funcctx->tuple_desc = tupdesc;

        MemoryContextSwitchTo(oldcontext);
    }

    // Per-call setup
    funcctx = SRF_PERCALL_SETUP();
    pindex = (int *)funcctx->user_fctx;

    // Check if we're done iterating
    if (zoneabbrevtbl == NULL || *pindex >= zoneabbrevtbl->numabbrevs)
        SRF_RETURN_DONE(funcctx);

    const datetkn *tp = zoneabbrevtbl->abbrevs + *pindex;
    int gmtoffset;
    bool is_dst;

    // Determine offset and DST status based on timezone type
    switch (tp->type) {
        case TZ:
            gmtoffset = tp->value;
            is_dst = false;
            break;
        case DTZ:
            gmtoffset = tp->value;
            is_dst = true;
            break;
        case DYNTZ:
            // Dynamic timezone - resolve at current time
            pg_tz *tzp;
            DateTimeErrorExtra extra;
            TimestampTz now;
            int isdst;

            tzp = FetchDynamicTimeZone(zoneabbrevtbl, tp, &extra);
            if (tzp == NULL)
                DateTimeParseError(DTERR_BAD_ZONE_ABBREV, &extra, NULL, NULL, NULL);

            now = GetCurrentTransactionStartTimestamp();
            gmtoffset = -DetermineTimeZoneAbbrevOffsetTS(now, tp->token, tzp, &isdst);
            is_dst = (bool)isdst;
            break;
        default:
            elog(ERROR, "unrecognized timezone type %d", (int)tp->type);
    }

    // Convert abbreviation to uppercase
    char buffer[TOKMAXLEN + 1];
    strlcpy(buffer, tp->token, sizeof(buffer));
    for (unsigned char *p = (unsigned char *)buffer; *p; p++)
        *p = pg_toupper(*p);

    // Build result tuple
    Datum values[3];
    bool nulls[3] = {0};

    values[0] = CStringGetTextDatum(buffer);

    // Convert offset to interval
    struct pg_itm_in itm_in;
    MemSet(&itm_in, 0, sizeof(struct pg_itm_in));
    itm_in.tm_usec = (int64)gmtoffset * USECS_PER_SEC;
    Interval *resInterval = (Interval *)palloc(sizeof(Interval));
    (void)itmin2interval(&itm_in, resInterval);
    values[1] = IntervalPGetDatum(resInterval);

    values[2] = BoolGetDatum(is_dst);

    (*pindex)++;

    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);

    SRF_RETURN_NEXT(funcctx, result);
}
```