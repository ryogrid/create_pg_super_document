# outzone

## Location
[src/timezone/zic.c:2946-3061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2946-L3061)

## Overview
The main function that generates binary timezone data for a timezone zone, creating the transition times, types, and POSIX timezone string for output to timezone files.

## Definition
```c
static void outzone(const struct zone *zpfirst, ptrdiff_t zonecount)
```

## Detailed Description
The `outzone` function is the core data generation engine of the timezone compiler (zic). It processes a series of timezone zones to create all the necessary timezone data structures including:

- Transition times when timezone rules change
- Timezone types (standard/daylight combinations)
- Timezone abbreviations and their character storage
- POSIX timezone string for future rule extrapolation
- Leap second adjustments if applicable

The function handles complex scenarios like:
- Multiple rule transitions within and across years
- Year 2038 boundary considerations for 32-bit systems
- Extension of timezone data for future years beyond explicit rules
- Compatibility with various POSIX timezone string formats
- Memory allocation for abbreviation and environment variable storage

## Parameters / Member Variables
- `zpfirst`: Pointer to the first zone structure in an array of related zones
- `zonecount`: Number of zones in the array to process

## Dependencies
- Functions called/Symbols referenced:
  - `[emalloc](../e/emalloc.md)` (memory allocation wrapper)
  - `INITIALIZE` (macro for variable initialization)
  - [updateminmax](../u/updateminmax.md) (updates minimum/maximum year ranges)
  - [stringzone](../s/stringzone.md) (generates POSIX timezone string representation)
  - [warning](../w/warning.md) (issues warning messages)
- Called from (representative examples):
  - [main](../m/main.md) (primary entry point of the zic timezone compiler)

## Notes and Other Information
- Sets global variables like `timecnt`, `typecnt`, and `charcnt` for timezone data counts
- Allocates memory for timezone abbreviations and environment variables
- Determines timezone file version based on POSIX compatibility requirements
- Handles the year 2038 problem by using appropriate data types and boundaries
- Implements the 400-year repetition cycle for timezone rules per POSIX standards
- Generates warnings for zones that cannot be represented as POSIX timezone strings
- Critical component of PostgreSQL's timezone data compilation system

## Simplified Source

```c
static void
outzone(const struct zone *zpfirst, ptrdiff_t zonecount)
{
    const struct zone *zp;
    struct rule *rp;
    ptrdiff_t i, j;
    zic_t starttime, untiltime;
    zic_t stdoff, save, year;
    char *startbuf, *ab, *envvar;
    int max_abbr_len, max_envvar_len;
    bool prodstic, do_extend;
    char version;
    int defaulttype = -1;

    // Calculate buffer sizes and allocate memory
    max_abbr_len = 2 + max_format_len + max_abbrvar_len;
    max_envvar_len = 2 * max_abbr_len + 5 * 9;
    startbuf = emalloc(max_abbr_len + 1);
    ab = emalloc(max_abbr_len + 1);
    envvar = emalloc(max_envvar_len + 1);

    // Initialize global counters and determine year range
    timecnt = typecnt = charcnt = 0;
    prodstic = (zonecount == 1);
    min_year = max_year = EPOCH_YEAR;

    // Process leap seconds if present
    if (leapseen) {
        updateminmax(leapminyear);
        updateminmax(leapmaxyear + (leapmaxyear < ZIC_MAX));
    }

    // Scan all zones to determine complete year range
    for (i = 0; i < zonecount; ++i) {
        zp = &zpfirst[i];
        if (i < zonecount - 1)
            updateminmax(zp->z_untilrule.r_loyear);

        for (j = 0; j < zp->z_nrules; ++j) {
            rp = &zp->z_rules[j];
            if (rp->r_lowasnum)
                updateminmax(rp->r_loyear);
            if (rp->r_hiwasnum)
                updateminmax(rp->r_hiyear);
            if (rp->r_lowasnum || rp->r_hiwasnum)
                prodstic = false;
        }
    }

    // Generate POSIX timezone string and determine compatibility
    compat = stringzone(envvar, zpfirst, zonecount);
    version = (compat < 2013) ? ZIC_VERSION_PRE_2013 : ZIC_VERSION;
    do_extend = (compat < 0);

    // Extend year range if needed for comprehensive timezone data
    if (do_extend) {
        // Add extra years to handle edge cases in 400-year cycles
        // [Additional logic for year range extension]
    }

    // Main zone processing loop
    for (i = 0; i < zonecount; ++i) {
        zp = &zpfirst[i];
        stdoff = zp->z_stdoff;

        if (zp->z_nrules == 0) {
            // Simple zone without DST rules
            save = zp->z_save;
            doabbr(startbuf, zp, NULL, zp->z_isdst, save, false);
            type = addtype(oadd(zp->z_stdoff, save), startbuf,
                          zp->z_isdst, startttisstd, startttisut);
        } else {
            // Complex zone with DST rules - process year by year
            for (year = min_year; year <= max_year; ++year) {
                // [Process all rules for this year and zone]
                // [Generate transition times and types]
            }
        }
    }

    // Clean up allocated memory
    free(startbuf);
    free(ab);
    free(envvar);
}
```