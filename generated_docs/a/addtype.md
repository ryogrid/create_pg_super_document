# addtype

## Location
[src/timezone/zic.c:3358-3402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3358-L3402)

## Overview
The addtype function creates or retrieves timezone type entries, managing the global timezone type registry with deduplication to ensure each unique timezone configuration has a single type identifier.

## Definition

```c
static int
addtype(zic_t utoff, char const *abbr, bool isdst, bool ttisstd, bool ttisut)
```
## Detailed Description
The addtype function is a core component of PostgreSQL's timezone compiler that manages timezone type definitions. It handles timezone abbreviations, UTC offsets, daylight saving time flags, and standard/universal time indicators. The function first validates the UTC offset range, then searches for existing identical entries to avoid duplication. If no matching entry exists and the maximum type limit hasn't been reached, it creates a new timezone type entry. The function ensures timezone data integrity by deduplicating identical timezone configurations and enforcing system limits.

## Parameters / Member Variables
- `utoff`: A zic_t value representing the UTC offset in seconds for this timezone type
- `*abbr`: A constant character pointer to the timezone abbreviation string (e.g., "EST", "PDT")
- `isdst`: A boolean flag indicating whether this timezone type represents daylight saving time
- `ttisstd`: A boolean flag indicating whether this type uses standard time designation
- `ttisut`: A boolean flag indicating whether this type uses universal time designation
## Dependencies
- Functions called/Symbols referenced:
  - [want_bloat](../w/want_bloat.md) (configuration check)
  - [newabbr](../n/newabbr.md) (abbreviation management)
  - EXIT_FAILURE (error exit status)
  - TZ_MAX_TYPES (maximum type limit constant)
  - zic_t (timestamp type definition)
- Called from (representative examples):
  - [writezone](../w/writezone.md) (at lines 2375, 2387)
  - [years_of_observations](../y/years_of_observations.md) (at lines 3118, 3261, 3287)

## Notes and Other Information
- Returns the index of the timezone type (either existing or newly created)
- Validates UTC offset range: must be between -2147483648 and 2147483647 seconds
- Implements deduplication by comparing all timezone type attributes before creating new entries
- Exits with failure if UTC offset is out of range or if TZ_MAX_TYPES limit is exceeded
- Uses global arrays: utoffs, isdsts, ttisstds, ttisuts, desigidx for storing timezone type data
- The want_bloat() check can disable ttisstd and ttisut flags for minimal builds

## Simplified Source

```c
static int addtype(zic_t utoff, char const *abbr, bool isdst, bool ttisstd, bool ttisut) {
    int i, j;

    // Validate UTC offset range
    if (!(-1L - 2147483647L <= utoff && utoff <= 2147483647L)) {
        error(_("UT offset out of range"));
        exit(EXIT_FAILURE);
    }

    // Disable flags for minimal builds
    if (!want_bloat())
        ttisstd = ttisut = false;

    // Find or add the abbreviation string
    for (j = 0; j < charcnt; ++j)
        if (strcmp(&chars[j], abbr) == 0)
            break;

    if (j == charcnt) {
        newabbr(abbr);  // Add new abbreviation
    } else {
        // Check if identical type already exists
        for (i = 0; i < typecnt; i++) {
            if (utoff == utoffs[i] && isdst == isdsts[i] &&
                j == desigidx[i] && ttisstd == ttisstds[i] &&
                ttisut == ttisuts[i]) {
                return i;  // Return existing type index
            }
        }
    }

    // Check if we can add a new type
    if (typecnt >= TZ_MAX_TYPES) {
        error(_("too many local time types"));
        exit(EXIT_FAILURE);
    }

    // Add new timezone type
    i = typecnt++;
    utoffs[i] = utoff;
    isdsts[i] = isdst;
    ttisstds[i] = ttisstd;
    ttisuts[i] = ttisut;
    desigidx[i] = j;

    return i;
}
```