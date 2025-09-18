# zone_name_pref

## Location
src/bin/initdb/findtimezone.c: 615 - 656

## Overview
Determines the preference ranking for timezone names when multiple names provide equally good matches to the system timezone behavior.

## Definition
```c
static int zone_name_pref(const char *zonename)
```

## Detailed Description
This function assigns preference scores to timezone names to help select the most appropriate name when multiple timezone candidates have identical behavioral matches. It implements a preference hierarchy that favors standard timezone names over alternatives and penalizes pseudo-timezone names. The function returns higher values for more preferred timezone names, with 0 representing neutral preference. UTC variants receive high positive scores, while pseudo-names like "localtime" and "posixrules" receive negative scores to discourage their selection unless no other options exist.

## Parameters / Member Variables
- `zonename`: The timezone name to evaluate for preference ranking

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison function)
- Called from (representative examples):
  - scan_available_timezones (called twice)

## Notes and Other Information
- Returns 50 for "UTC" (highest preference)
- Returns 40 for "Etc/UTC" (high preference, but less than UTC)
- Returns -50 for "localtime" and "posixrules" (discouraged pseudo-names)
- Returns 0 for all other timezone names (neutral preference)
- Used during timezone selection to break ties when multiple zones have identical behavioral scores
- Part of the strategy to prefer canonical timezone names over aliases or system-specific names
- Helps ensure that PostgreSQL installations use standard, portable timezone names when possible