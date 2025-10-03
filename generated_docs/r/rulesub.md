# rulesub

## Location
[src/timezone/zic.c:1823-1991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1823-L1991)

## Overview
Parses and validates individual timezone rule components including years, months, days, and time specifications for daylight saving time transitions.

## Definition

```c
static void
rulesub(struct rule *rp, const char *loyearp, const char *hiyearp,
		const char *typep, const char *monthp, const char *dayp,
		const char *timep)
```
## Detailed Description
The  function is a core component of PostgreSQL's timezone compiler () responsible for parsing and validating the complex components of timezone rules. It processes the various fields that define when daylight saving time transitions occur, including year ranges, month names, day specifications (which can be complex expressions like "last Sunday" or "Sun>=7"), and time-of-day specifications with timezone indicators.

The function performs extensive validation and parsing of each component, handling special keywords like "minimum", "maximum", and "only" for years, parsing month names, interpreting complex day-of-month expressions including weekday-relative specifications, and parsing time specifications with optional timezone suffixes (s/w/g/u/z for standard/wall/Greenwich/Universal/Zulu time).

All parsed information is stored in the provided rule structure for later use in generating the compiled timezone data.

## Parameters / Member Variables
- `*rp`: Pointer to rule structure to populate with parsed information
- `*loyearp`: String specifying the starting year (can be numeric or keyword like "minimum")
- `*hiyearp`: String specifying the ending year (can be numeric, keyword, or "only")
- `*typep`: Year type specification (must be empty string in modern usage)
- `*monthp`: Month name (e.g., "Jan", "February") to be looked up in month name table
- `*dayp`: Day specification (can be numeric, "lastSunday", "Sun>=7", "Sun<=20", etc.)
- `*timep`: Time specification with optional suffix (e.g., "2:00", "2:00s", "2:00w")
## Dependencies
- Functions called/Symbols referenced:
  - [byword](../b/byword.md) (to lookup month names, year keywords, weekday names)
  - [ecpyalloc](../e/ecpyalloc.md) (to allocate temporary string copies for parsing)
  - [lowerit](../l/lowerit.md) (to normalize case for time suffix parsing)
  - [gethms](../g/gethms.md) (to parse hour:minute:second time specifications)
  - [error](../e/error.md) (for reporting parsing errors)
  - free (to deallocate temporary strings)
  - sscanf, strlen, strchr (standard string processing functions)
  - fprintf, exit (for fatal error handling)
- Called from (representative examples):
  - [inrule](../i/inrule.md) (when processing Rule lines)
  - [inzsub](../i/inzsub.md) (when processing Zone continuation lines)

## Notes and Other Information
- This function handles the most complex parsing in the timezone compiler, dealing with various date/time specification formats
- Day specifications support multiple formats: numeric day-of-month, "last" + weekday, weekday + comparison operator + day
- Time suffixes indicate the timezone context: 's' = standard time, 'w' = wall clock time, 'g'/'u'/'z' = UTC variants
- Year ranges can span from ZIC_MIN to ZIC_MAX, handling both numeric years and special keywords
- The function validates that start years don't exceed end years and that day-of-month values are valid for the specified month
- Extensive error checking ensures malformed rule specifications are caught during compilation rather than causing runtime issues
- Year type specifications are deprecated and must be empty in modern timezone database files

## Simplified Source

```c
static void rulesub(struct rule *rp, const char *loyearp, const char *hiyearp,
                   const char *typep, const char *monthp, const char *dayp,
                   const char *timep) {
    const struct lookup *lp;
    char *dp, *ep;
    int year_tmp;
    char xs;

    // Parse and validate month
    if ((lp = byword(monthp, mon_names)) == NULL) {
        error(_("invalid month name"));
        return;
    }
    rp->r_month = lp->l_value;

    // Parse time with timezone indicators
    rp->r_todisstd = false;
    rp->r_todisut = false;
    dp = ecpyalloc(timep);
    if (*dp != '\0') {
        ep = dp + strlen(dp) - 1;
        switch (lowerit(*ep)) {
            case 's':  // Standard time
                rp->r_todisstd = true;
                rp->r_todisut = false;
                *ep = '\0';
                break;
            case 'w':  // Wall clock time
                rp->r_todisstd = false;
                rp->r_todisut = false;
                *ep = '\0';
                break;
            case 'g': case 'u': case 'z':  // UTC variants
                rp->r_todisstd = true;
                rp->r_todisut = true;
                *ep = '\0';
                break;
        }
    }
    rp->r_tod = gethms(dp, _("invalid time of day"));
    free(dp);

    // Parse start year
    lp = byword(loyearp, begin_years);
    rp->r_lowasnum = lp == NULL;
    if (!rp->r_lowasnum) {
        switch (lp->l_value) {
            case YR_MINIMUM: rp->r_loyear = ZIC_MIN; break;
            case YR_MAXIMUM: rp->r_loyear = ZIC_MAX; break;
            default:
                fprintf(stderr, _("%s: panic: Invalid l_value %d\n"), progname, lp->l_value);
                exit(EXIT_FAILURE);
        }
    } else if (sscanf(loyearp, "%d%c", &year_tmp, &xs) == 1) {
        rp->r_loyear = year_tmp;
    } else {
        error(_("invalid starting year"));
        return;
    }

    // Parse end year
    lp = byword(hiyearp, end_years);
    rp->r_hiwasnum = lp == NULL;
    if (!rp->r_hiwasnum) {
        switch (lp->l_value) {
            case YR_MINIMUM: rp->r_hiyear = ZIC_MIN; break;
            case YR_MAXIMUM: rp->r_hiyear = ZIC_MAX; break;
            case YR_ONLY: rp->r_hiyear = rp->r_loyear; break;
            default:
                fprintf(stderr, _("%s: panic: Invalid l_value %d\n"), progname, lp->l_value);
                exit(EXIT_FAILURE);
        }
    } else if (sscanf(hiyearp, "%d%c", &year_tmp, &xs) == 1) {
        rp->r_hiyear = year_tmp;
    } else {
        error(_("invalid ending year"));
        return;
    }

    // Validate year range
    if (rp->r_loyear > rp->r_hiyear) {
        error(_("starting year greater than ending year"));
        return;
    }

    // Validate type field (must be empty)
    if (*typep != '\0') {
        error(_("year type \"%s\" is unsupported; use \"-\" instead"), typep);
        return;
    }

    // Parse day specification
    dp = ecpyalloc(dayp);
    if ((lp = byword(dp, lasts)) != NULL) {
        // "last" + weekday format
        rp->r_dycode = DC_DOWLEQ;
        rp->r_wday = lp->l_value;
        rp->r_dayofmonth = len_months[1][rp->r_month];
    } else {
        // Parse day-of-month with optional weekday constraints
        if ((ep = strchr(dp, '<')) != NULL)
            rp->r_dycode = DC_DOWLEQ;
        else if ((ep = strchr(dp, '>')) != NULL)
            rp->r_dycode = DC_DOWGEQ;
        else {
            ep = dp;
            rp->r_dycode = DC_DOM;
        }

        if (rp->r_dycode != DC_DOM) {
            *ep++ = 0;
            if (*ep++ != '=' || (lp = byword(dp, wday_names)) == NULL) {
                error(_("invalid day of month"));
                free(dp);
                return;
            }
            rp->r_wday = lp->l_value;
        }

        if (sscanf(ep, "%d%c", &rp->r_dayofmonth, &xs) != 1 ||
            rp->r_dayofmonth <= 0 ||
            (rp->r_dayofmonth > len_months[1][rp->r_month])) {
            error(_("invalid day of month"));
            free(dp);
            return;
        }
    }
    free(dp);
}
```