# stringzone

## Location
[src/timezone/zic.c:2811-2945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2811-L2945)

## Overview
Generates a complete POSIX timezone string representation for a timezone zone, including standard and daylight saving time rules and their transition dates.

## Definition
```c
static int stringzone(char *result, struct zone const *zpfirst, ptrdiff_t zonecount)
```

## Detailed Description
The `stringzone` function creates a POSIX-compliant timezone string (TZ string) for a given timezone zone. It analyzes the zone's rules to extract standard time and daylight saving time information, then formats them into a string like "EST5EDT,M3.2.0,M11.1.0" (Eastern Time example).

The function handles several complex scenarios:
- Zones with both standard and daylight saving time rules
- Zones with only standard time (no DST)
- Perpetual daylight saving time zones
- Time offset calculations and abbreviation formatting

It follows RFC 8536 requirements for TZ string generation and returns a compatibility year indicating the minimum POSIX version needed to support the generated string.

## Parameters / Member Variables
- `result`: Output buffer where the complete timezone string will be written
- `zpfirst`: Pointer to the first zone structure in an array of zones
- `zonecount`: Number of zones in the array (function processes the last zone)

## Dependencies
- Functions called/Symbols referenced:
  - [rule_cmp](../r/rule_cmp.md) (compares timezone rules for sorting)
  - [doabbr](../d/doabbr.md) (formats timezone abbreviations)
  - [stringoffset](stringoffset.md) (formats time offsets)
  - [stringrule](stringrule.md) (formats daylight saving time transition rules)
  - `strlen` (standard library string length function)
- Called from (representative examples):
  - [outzone](../o/outzone.md) (outputs timezone data to files)

## Notes and Other Information
- Returns -1 if the timezone cannot be represented as a POSIX TZ string
- Returns a compatibility year (0, 1994, or 2013) for successful conversions
- Handles perpetual DST by creating synthetic standard time rules
- Follows Internet RFC 8536 section 5.1 for truncated future timestamps
- Processes the last zone in the provided zone array
- Generates strings in the format: `STD[offset][DST[offset],start[/time],end[/time]]`
- Part of PostgreSQL's timezone compilation system for creating binary timezone files

## Simplified Source

```c
static int stringzone(char *result, struct zone const *zpfirst, ptrdiff_t zonecount) {
    const struct zone *zp;
    struct rule *stdrp, *dstrp;
    ptrdiff_t i;
    int compat = 0;
    size_t len;
    int offsetlen;

    result[0] = '\0';

    // Check for truncated future timestamps (RFC 8536)
    if (hi_time < max_time)
        return -1;

    // Process the last zone in the array
    zp = zpfirst + zonecount - 1;
    stdrp = dstrp = NULL;

    // Find standard and daylight saving time rules
    for (i = 0; i < zp->z_nrules; ++i) {
        struct rule *rp = &zp->z_rules[i];

        // Skip rules that don't run through "max"
        if (rp->r_hiwasnum || rp->r_hiyear != ZIC_MAX)
            continue;

        if (!rp->r_isdst) {
            if (stdrp == NULL)
                stdrp = rp;
            else
                return -1;  // Multiple standard time rules
        } else {
            if (dstrp == NULL)
                dstrp = rp;
            else
                return -1;  // Multiple DST rules
        }
    }

    // Handle case with no perpetual rules
    if (stdrp == NULL && dstrp == NULL) {
        // Find latest rules
        struct rule *stdabbrrp = NULL;
        for (i = 0; i < zp->z_nrules; ++i) {
            struct rule *rp = &zp->z_rules[i];
            if (!rp->r_isdst && rule_cmp(stdabbrrp, rp) < 0)
                stdabbrrp = rp;
            if (rule_cmp(stdrp, rp) < 0)
                stdrp = rp;
        }

        // Handle perpetual DST by creating synthetic rules
        if (stdrp != NULL && stdrp->r_isdst) {
            // Create synthetic DST and standard rules
            // ... (simplified synthetic rule creation)
        }
    }

    // Build the TZ string
    if (stdrp == NULL && (zp->z_nrules != 0 || zp->z_isdst))
        return -1;

    // Add standard time abbreviation and offset
    const char *abbrvar = (stdrp == NULL) ? "" : stdrp->r_abbrvar;
    len = doabbr(result, zp, abbrvar, false, 0, true);

    offsetlen = stringoffset(result + len, -zp->z_stdoff);
    if (!offsetlen) {
        result[0] = '\0';
        return -1;
    }
    len += offsetlen;

    // If no DST, we're done
    if (dstrp == NULL)
        return compat;

    // Add DST abbreviation and offset
    len += doabbr(result + len, zp, dstrp->r_abbrvar,
                  dstrp->r_isdst, dstrp->r_save, true);

    if (dstrp->r_save != SECSPERMIN * MINSPERHOUR) {
        offsetlen = stringoffset(result + len,
                                -(zp->z_stdoff + dstrp->r_save));
        if (!offsetlen) {
            result[0] = '\0';
            return -1;
        }
        len += offsetlen;
    }

    // Add DST transition rules
    result[len++] = ',';
    int c = stringrule(result + len, dstrp, dstrp->r_save, zp->z_stdoff);
    if (c < 0) {
        result[0] = '\0';
        return -1;
    }
    if (compat < c) compat = c;

    len += strlen(result + len);
    result[len++] = ',';

    c = stringrule(result + len, stdrp, dstrp->r_save, zp->z_stdoff);
    if (c < 0) {
        result[0] = '\0';
        return -1;
    }
    if (compat < c) compat = c;

    return compat;
}
```