# pg_interpret_timezone_abbrev

## Location
[src/timezone/localtime.c:1757-1850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1757-L1850)

## Overview
This function identifies a timezone abbreviation's meaning within a specific timezone, determining the GMT offset and DST flag associated with the abbreviation at or around a given time.

## Definition

```c
struct state *sp;
```
## Detailed Description
pg_interpret_timezone_abbrev resolves the meaning of a timezone abbreviation within a given timezone, particularly when the abbreviation has changed meaning over time. The function takes a UTC cutoff time and returns the meaning in use at or most recently before that time, or the first meaning after that time if the abbreviation was never used before the cutoff.

The function performs a binary search to locate time transitions and then scans backwards from the cutoff time to find the latest interval using the given abbreviation. If no match is found before the cutoff, it scans forward to find the first usage after the cutoff time.

The abbreviation matching is case-sensitive and should be provided in all-upper-case format.

## Parameters / Member Variables
- : The timezone abbreviation to interpret (case-sensitive, should be all-upper-case)
- : Pointer to the UTC cutoff time for determining which meaning to use
- : Output parameter for the GMT offset in seconds associated with the abbreviation
- : Output parameter indicating whether daylight saving time is in effect (1 for DST, 0 for standard time)
- : The timezone structure containing the timezone data to search within

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (time type)
  - [pg_tz](pg_tz.md) (timezone structure type)
  - [ttinfo](../t/ttinfo.md) (timezone transition info structure)
  - strcmp (standard C string comparison)
- Called from (representative examples):
  - [DetermineTimeZoneAbbrevOffsetInternal](../D/DetermineTimeZoneAbbrevOffsetInternal.md) (src/backend/utils/adt/datetime.c:1834)

## Notes and Other Information
- Returns true on success with gmtoff and isdst set to appropriate values
- Returns false if the abbreviation was never used in the specified timezone
- The function assumes no duplicate abbreviations exist in the timezone's abbreviation list
- Uses binary search for efficient time transition lookup
- Does not require handling of extrapolation zones (goback/goahead) since finding newest/oldest meanings suffices
- Located in src/timezone/localtime.c:1757-1850

## Simplified Source

```c
bool pg_interpret_timezone_abbrev(const char *abbrev,
                                const pg_time_t *timep,
                                long int *gmtoff,
                                int *isdst,
                                const pg_tz *tz) {
    const struct state *sp = &tz->state;
    const char *abbrs = sp->chars;
    int abbrind = 0;
    const pg_time_t t = *timep;

    // Find the abbreviation in the timezone's abbreviation list
    while (abbrind < sp->charcnt) {
        if (strcmp(abbrev, abbrs + abbrind) == 0)
            break;
        while (abbrs[abbrind] != '\0')
            abbrind++;
        abbrind++;
    }
    if (abbrind >= sp->charcnt)
        return false;  // Abbreviation not found

    // Binary search to find the cutoff time position
    int lo = 0, hi = sp->timecnt;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        if (t < sp->ats[mid])
            hi = mid;
        else
            lo = mid + 1;
    }
    int cutoff = lo;

    // Search backwards for the latest use of this abbreviation before cutoff
    for (int i = cutoff - 1; i >= 0; i--) {
        const struct ttinfo *ttisp = &sp->ttis[sp->types[i]];
        if (ttisp->tt_desigidx == abbrind) {
            *gmtoff = ttisp->tt_utoff;
            *isdst = ttisp->tt_isdst;
            return true;
        }
    }

    // Not found before cutoff, search forward for first use after cutoff
    for (int i = cutoff; i < sp->timecnt; i++) {
        const struct ttinfo *ttisp = &sp->ttis[sp->types[i]];
        if (ttisp->tt_desigidx == abbrind) {
            *gmtoff = ttisp->tt_utoff;
            *isdst = ttisp->tt_isdst;
            return true;
        }
    }

    return false;  // Abbreviation not used in any interval
}
```