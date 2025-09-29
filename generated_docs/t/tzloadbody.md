# tzloadbody

## Location
[src/timezone/localtime.c:211-585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L211-L585)

## Overview
Loads timezone data from a timezone database file into a timezone state structure, parsing both the binary timezone data format and optional POSIX timezone strings.

## Definition

```c
static int
tzloadbody(char const *name, char *canonname, struct state *sp, bool doextend,
		   union local_storage *lsp)
```
## Detailed Description
The `tzloadbody` function is the core timezone file parser in PostgreSQL's timezone system. It reads and validates timezone data files (tzfile format), parsing binary data that includes transition times, timezone types, leap second information, and timezone abbreviations.

The function supports both 32-bit and 64-bit timestamp formats, and can handle extended POSIX timezone strings for future transitions beyond the file's explicit data. It performs extensive validation of the data and applies various compatibility workarounds for older timezone database formats.

The function processes timezone files in multiple passes to handle both 32-bit (4-byte) and 64-bit (8-byte) timestamp formats, discarding transitions outside the representable time range and optimizing the data for efficient lookups.

## Parameters / Member Variables
- `name`: Name of the timezone file to load (NULL defaults to TZDEFAULT)
- `canonname`: Buffer to store the canonical name of the timezone (must be > TZ_STRLEN_MAX bytes, can be NULL)
- `sp`: Pointer to the state structure to populate with timezone data
- `doextend`: Whether to process extended POSIX timezone strings for future transitions
- `lsp`: Temporary storage for file I/O and parsing operations

## Dependencies
- Functions called/Symbols referenced:
  - [pg_open_tzfile](../p/pg_open_tzfile.md) (opens timezone files)
  - [detzcode](../d/detzcode.md), detzcode64 (decode big-endian integers)
  - [differ_by_repeat](../d/differ_by_repeat.md) (check for repeating patterns)
  - [typesequiv](typesequiv.md) (check timezone type equivalence)
  - [tzparse](tzparse.md) (parse POSIX timezone strings)
  - [leapcorr](../l/leapcorr.md) (leap second corrections)
- Called from (representative examples):
  - [tzload](tzload.md) (single caller at line 594)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c compilation unit
- Supports multiple file format versions and handles both 32-bit and 64-bit timestamps
- Includes extensive validation of timezone file data to prevent malformed input
- Implements compatibility workarounds for timezone data from different eras (pre-2013c, 2018e, etc.)
- Optimizes timezone transitions by detecting and utilizing repeating patterns (400-year cycles)
- Handles leap second data with validation for proper timing and corrections
- Can extend timezone data with POSIX timezone strings for transitions beyond the file's explicit data
- Returns 0 on success, or an errno value on failure (EINVAL, ENOENT, etc.)
- The function is quite large (375 lines) due to the complexity of the timezone file format and various edge cases
- Includes special handling for timezone abbreviation reuse to minimize memory usage

## Simplified Source

```c
// Simplified version of tzloadbody
static int
tzloadbody(char const *name, char *canonname, struct state *sp, bool doextend,
           union local_storage *lsp) {
    int fid, stored, timecnt, leapcnt;
    ssize_t nread;
    union input_buffer *up = &lsp->u.u;

    // Initialize state
    sp->goback = sp->goahead = false;

    // Handle default timezone name
    if (!name) {
        name = TZDEFAULT;
        if (!name) return EINVAL;
    }

    // Skip colon prefix if present
    if (name[0] == ':') ++name;

    // Open timezone file
    fid = pg_open_tzfile(name, canonname);
    if (fid < 0) return ENOENT;

    // Read file data
    nread = read(fid, up->buf, sizeof up->buf);
    if (nread < sizeof(struct tzhead)) {
        close(fid);
        return nread < 0 ? errno : EINVAL;
    }
    close(fid);

    // Parse both 32-bit and 64-bit formats
    for (stored = 4; stored <= 8; stored *= 2) {
        // Decode header fields
        int32 leapcnt = detzcode(up->tzhead.tzh_leapcnt);
        int32 timecnt = detzcode(up->tzhead.tzh_timecnt);
        int32 typecnt = detzcode(up->tzhead.tzh_typecnt);
        int32 charcnt = detzcode(up->tzhead.tzh_charcnt);
        char const *p = up->buf + sizeof(struct tzhead);

        // Validate data ranges
        if (!(0 <= leapcnt && leapcnt < TZ_MAX_LEAPS &&
              0 <= typecnt && typecnt < TZ_MAX_TYPES &&
              0 <= timecnt && timecnt < TZ_MAX_TIMES &&
              0 <= charcnt && charcnt < TZ_MAX_CHARS)) {
            return EINVAL;
        }

        // Store counts in state
        sp->leapcnt = leapcnt;
        sp->timecnt = timecnt;
        sp->typecnt = typecnt;
        sp->charcnt = charcnt;

        // Read transition times, filter valid range
        timecnt = 0;
        for (int i = 0; i < sp->timecnt; ++i) {
            int64 at = (stored == 4) ? detzcode(p) : detzcode64(p);
            sp->types[i] = (at <= TIME_T_MAX);
            if (sp->types[i]) {
                pg_time_t attime = (at < TIME_T_MIN) ? TIME_T_MIN : at;
                // Handle duplicate times
                if (timecnt && attime <= sp->ats[timecnt - 1]) {
                    if (attime < sp->ats[timecnt - 1]) return EINVAL;
                    sp->types[i - 1] = 0;
                    timecnt--;
                }
                sp->ats[timecnt++] = attime;
            }
            p += stored;
        }

        // Read transition types
        timecnt = 0;
        for (int i = 0; i < sp->timecnt; ++i) {
            unsigned char typ = *p++;
            if (sp->typecnt <= typ) return EINVAL;
            if (sp->types[i]) sp->types[timecnt++] = typ;
        }
        sp->timecnt = timecnt;

        // Read timezone type information
        for (int i = 0; i < sp->typecnt; ++i) {
            sp->ttis[i].tt_utoff = detzcode(p);
            p += 4;
            sp->ttis[i].tt_isdst = *p++;
            sp->ttis[i].tt_desigidx = *p++;
        }

        // Read timezone abbreviation strings
        for (int i = 0; i < sp->charcnt; ++i) {
            sp->chars[i] = *p++;
        }
        sp->chars[sp->charcnt] = '\0';

        // Read leap seconds within valid range
        leapcnt = 0;
        for (int i = 0; i < sp->leapcnt; ++i) {
            int64 tr = (stored == 4) ? detzcode(p) : detzcode64(p);
            int32 corr = detzcode(p + stored);
            p += stored + 4;

            if (tr >= 0 && tr <= TIME_T_MAX) {
                sp->lsis[leapcnt].ls_trans = tr;
                sp->lsis[leapcnt].ls_corr = corr;
                leapcnt++;
            }
        }
        sp->leapcnt = leapcnt;

        // Read standard/UTC flags if present
        for (int i = 0; i < sp->typecnt; ++i) {
            sp->ttis[i].tt_ttisstd = (i < ttisstdcnt) ? *p++ : false;
        }
        for (int i = 0; i < sp->typecnt; ++i) {
            sp->ttis[i].tt_ttisut = (i < ttisutcnt) ? *p++ : false;
        }

        // Break if old format (version 1)
        if (up->tzhead.tzh_version[0] == '\0') break;

        // Prepare for next iteration (64-bit format)
        nread -= p - up->buf;
        memmove(up->buf, p, nread);
    }

    // Handle extended POSIX timezone string
    if (doextend && nread > 2 && up->buf[0] == '\n') {
        up->buf[nread - 1] = '\0';
        struct state *ts = &lsp->u.st;
        if (tzparse(&up->buf[1], ts, false)) {
            // Merge parsed data with existing state
            // (Complex abbreviation reuse and transition merging)
        }
    }

    // Validate we have at least one timezone type
    if (sp->typecnt == 0) return EINVAL;

    // Detect repeating patterns for optimization
    if (sp->timecnt > 1) {
        // Check for backward and forward repeat patterns
        for (int i = 1; i < sp->timecnt; ++i) {
            if (typesequiv(sp, sp->types[i], sp->types[0]) &&
                differ_by_repeat(sp->ats[i], sp->ats[0])) {
                sp->goback = true;
                break;
            }
        }
        // Similar check for forward patterns
    }

    // Determine default timezone type for pre-transition times
    sp->defaulttype = 0; // Simplified - use first type

    return 0; // Success
}
```

Key simplifications made:
- Removed complex error handling details for clarity
- Simplified timestamp range validation logic
- Abstracted complex POSIX timezone string merging
- Condensed leap second validation to essential checks
- Simplified default type determination algorithm
- Removed detailed memory layout calculations
- Consolidated similar processing loops
- Added high-level comments explaining major steps
- Reduced from 375 lines to approximately 120 lines