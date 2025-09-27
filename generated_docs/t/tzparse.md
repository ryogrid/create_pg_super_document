# tzparse

## Location
[src/timezone/localtime.c:936-1244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L936-L1244)

## Overview
Parses POSIX section 8-style timezone strings and populates timezone state structures with appropriate transition rules and offsets.

## Definition
```c
bool tzparse(const char *name, struct state *sp, bool lastditch)
```

## Detailed Description
The tzparse function is the main parser for POSIX timezone specifications. It handles complex timezone strings that define:

1. **Standard timezone abbreviation and offset** (required)
2. **Daylight saving timezone abbreviation and offset** (optional)
3. **Transition rules** specifying when DST begins and ends (optional)

The function supports several parsing modes:

**Standard Time Only**: Just timezone name and offset (e.g., "EST5")

**Standard + DST**: Two timezone names with offsets (e.g., "EST5EDT")

**Full POSIX Format**: Complete specification with transition rules (e.g., "EST5EDT,M3.2.0,M11.1.0")

**Special Handling**:
- Quoted timezone names using < > brackets
- Default DST offset (1 hour ahead of standard time)
- Default transition rules using TZDEFRULESTRING
- Year-by-year transition calculation for the epoch range
- Overflow protection for time calculations

The function generates a complete timezone state structure with transition times, timezone types, and abbreviation strings. It can handle both forward and backward transitions and supports repeating patterns for multi-year calculations.

## Parameters / Member Variables
- `name`: POSIX timezone string to parse (e.g., "PST8PDT,M3.2.0,M11.1.0")
- `sp`: Pointer to state structure that will be filled with parsed timezone information
- `lastditch`: Boolean flag indicating this is a fallback parsing attempt (affects validation)

## Dependencies
- Functions called/Symbols referenced:
  - [getqzname](../g/getqzname.md), getzname (for parsing timezone names)
  - [getoffset](../g/getoffset.md) (for parsing timezone offsets)
  - [getrule](../g/getrule.md) (for parsing transition rules)
  - [transtime](transtime.md) (for calculating actual transition times)
  - [init_ttinfo](../i/init_ttinfo.md) (for initializing timezone type information)
  - [increment_overflow_time](../i/increment_overflow_time.md) (for safe time arithmetic)
  - isleap (for leap year calculations)
  - EPOCH_YEAR, YEARSPERREPEAT, TZ_MAX_TIMES, SECSPERHOUR, SECSPERDAY (constants)
  - TZDEFRULESTRING (default transition rules)
- Called from (representative examples):
  - [pg_load_tz](../p/pg_load_tz.md) (from initdb)
  - [tzloadbody](tzloadbody.md)
  - [gmtload](../g/gmtload.md)
  - [pg_tzset](../p/pg_tzset.md)
  - [pg_tz](../p/pg_tz.md)

## Notes and Other Information
- Returns true on successful parsing, false on any error
- This is a public function used throughout the PostgreSQL timezone subsystem
- Unlike IANA reference implementation, doesn't load TZDEFRULES file for security and stability
- Assumes no leap seconds for POSIX compatibility
- Supports extended year ranges with overflow protection
- Handles edge cases like perpetual DST and reversed transitions
- Critical function for PostgreSQL's timezone support - used during database initialization and runtime timezone operations
- The function can generate transition tables covering multiple years of DST changes
- Supports both simple timezone offsets and complex recurring transition rules

## Simplified Source

```c
// Simplified version of tzparse
bool tzparse(const char *name, struct state *sp, bool lastditch) {
    const char *stdname;
    const char *dstname = NULL;
    size_t stdlen, dstlen;
    int32 stdoffset, dstoffset;
    char *cp;

    // Parse standard timezone name and offset
    stdname = name;
    if (lastditch) {
        // Simple case: treat entire name as timezone, offset = 0
        stdlen = strlen(name);
        name += stdlen;
        stdoffset = 0;
    } else {
        // Parse timezone name (quoted or unquoted)
        if (*name == '<') {
            name++;
            stdname = name;
            name = getqzname(name, '>');
            if (*name != '>') return false;
            stdlen = name - stdname;
            name++;
        } else {
            name = getzname(name);
            stdlen = name - stdname;
        }

        // Must have valid name and offset
        if (*name == '\0') return false;
        name = getoffset(name, &stdoffset);
        if (name == NULL) return false;
    }

    // Validate buffer space for timezone abbreviations
    size_t charcnt = stdlen + 1;
    if (sizeof sp->chars < charcnt) return false;

    // Initialize state structure defaults
    sp->goback = sp->goahead = false;
    sp->leapcnt = 0;  // No leap seconds for POSIX compatibility

    // Parse DST timezone if present
    if (*name != '\0') {
        // Parse DST timezone name
        if (*name == '<') {
            dstname = ++name;
            name = getqzname(name, '>');
            if (*name != '>') return false;
            dstlen = name - dstname;
            name++;
        } else {
            dstname = name;
            name = getzname(name);
            dstlen = name - dstname;
        }

        if (!dstlen) return false;
        charcnt += dstlen + 1;
        if (sizeof sp->chars < charcnt) return false;

        // Parse DST offset (defaults to 1 hour ahead of standard)
        if (*name != '\0' && *name != ',' && *name != ';') {
            name = getoffset(name, &dstoffset);
            if (name == NULL) return false;
        } else {
            dstoffset = stdoffset - SECSPERHOUR;
        }

        // Handle transition rules
        if (*name == '\0') {
            name = TZDEFRULESTRING;  // Use default rules
        }

        if (*name == ',' || *name == ';') {
            // Parse and process transition rules
            struct rule start, end;
            ++name;

            if ((name = getrule(name, &start)) == NULL) return false;
            if (*name++ != ',') return false;
            if ((name = getrule(name, &end)) == NULL) return false;
            if (*name != '\0') return false;

            // Set up timezone types
            sp->typecnt = 2;
            init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
            init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
            sp->defaulttype = 0;

            // Generate transition times for multiple years
            generate_transitions(sp, &start, &end, stdoffset, dstoffset);
        } else {
            // Handle pre-loaded timezone data (adjust for our offsets)
            adjust_existing_transitions(sp, stdoffset, dstoffset, stdlen);
        }
    } else {
        // Standard time only - no DST
        dstlen = 0;
        sp->typecnt = 1;
        sp->timecnt = 0;
        init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
        sp->defaulttype = 0;
    }

    // Copy timezone abbreviation strings to state structure
    sp->charcnt = charcnt;
    cp = sp->chars;
    memcpy(cp, stdname, stdlen);
    cp += stdlen;
    *cp++ = '\0';
    if (dstlen != 0) {
        memcpy(cp, dstname, dstlen);
        *(cp + dstlen) = '\0';
    }

    return true;
}

// Helper function: Generate year-by-year DST transitions
static void generate_transitions(struct state *sp, struct rule *start,
                                struct rule *end, int32 stdoffset, int32 dstoffset) {
    int timecnt = 0;
    pg_time_t janfirst = 0;
    int32 janoffset = 0;
    int yearbeg = EPOCH_YEAR;

    // Calculate starting point for transition generation
    while (EPOCH_YEAR - YEARSPERREPEAT / 2 < yearbeg) {
        int32 yearsecs = year_lengths[isleap(yearbeg - 1)] * SECSPERDAY;
        yearbeg--;
        if (increment_overflow_time(&janfirst, -yearsecs)) {
            janoffset = -yearsecs;
            break;
        }
    }

    // Generate transitions for each year in range
    int yearlim = yearbeg + YEARSPERREPEAT + 1;
    for (int year = yearbeg; year < yearlim; year++) {
        int32 starttime = transtime(year, start, stdoffset);
        int32 endtime = transtime(year, end, dstoffset);

        // Add valid transitions to the table
        if (valid_transition_pair(starttime, endtime, stdoffset, dstoffset)) {
            if (TZ_MAX_TIMES - 2 < timecnt) break;

            // Add start and end transitions for this year
            add_transition(&sp->ats[timecnt], &sp->types[timecnt],
                          janfirst, janoffset, starttime, endtime);
            timecnt += 2;
        }

        // Move to next year
        int32 yearsecs = year_lengths[isleap(year)] * SECSPERDAY;
        if (increment_overflow_time(&janfirst, janoffset + yearsecs)) break;
        janoffset = 0;
    }

    sp->timecnt = timecnt;
    if (!timecnt) {
        // No transitions - perpetual DST
        sp->ttis[0] = sp->ttis[1];
        sp->typecnt = 1;
    } else if (YEARSPERREPEAT < year - yearbeg) {
        sp->goback = sp->goahead = true;
    }
}

// Helper function: Adjust pre-existing timezone transitions
static void adjust_existing_transitions(struct state *sp, int32 stdoffset,
                                       int32 dstoffset, size_t stdlen) {
    // Find current standard and DST offsets in the data
    int32 theirstdoffset = find_std_offset(sp);
    int32 theirdstoffset = find_dst_offset(sp);

    // Adjust all transition times to match our desired offsets
    for (int i = 0; i < sp->timecnt; ++i) {
        int j = sp->types[i];
        sp->types[i] = sp->ttis[j].tt_isdst;

        if (!sp->ttis[j].tt_ttisut) {
            // Adjust transition time based on current DST state
            if (current_dst && !sp->ttis[j].tt_ttisstd) {
                sp->ats[i] += dstoffset - theirdstoffset;
            } else {
                sp->ats[i] += stdoffset - theirstdoffset;
            }
        }
    }

    // Set up final timezone type information
    init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
    init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
    sp->typecnt = 2;
    sp->defaulttype = 0;
}
```

Key simplifications made:
- Removed complex nested conditionals and combined similar logic paths
- Abstracted the complex transition generation into helper functions
- Simplified the DST offset adjustment logic into a separate helper
- Consolidated buffer validation and error checking
- Focused on the main parsing flow while preserving essential functionality
- Added descriptive comments for each major parsing phase
- Maintained all critical operations: name parsing, offset calculation, transition generation