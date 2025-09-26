# pgstat_get_kind_from_str

## Location
[src/backend/utils/activity/pgstat.c:1244-1258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1244-L1258)

## Overview
Converts a string representation of a statistics kind name into the corresponding PgStat_Kind enumeration value, providing case-insensitive string-to-enum mapping.

## Definition
PgStat_Kind pgstat_get_kind_from_str(char *kind_str)

## Detailed Description
This utility function performs a case-insensitive lookup to convert a string representation of a statistics kind (such as "database", "relation", "function", etc.) into the corresponding PgStat_Kind enumeration value. It iterates through all valid statistics kinds from PGSTAT_KIND_FIRST_VALID to PGSTAT_KIND_LAST, comparing the input string against the name field in the pgstat_kind_infos array using pg_strcasecmp for case-insensitive matching. If no match is found, it raises an ERROR with an appropriate error code and message. The function returns PGSTAT_KIND_DATABASE as a fallback to avoid compiler warnings, though this line should never be reached due to the ERROR being thrown.

## Parameters / Member Variables
- `kind_str`: A null-terminated string containing the name of the statistics kind to look up

## Dependencies
- Functions called/Symbols referenced:
  - pg_strcasecmp
  - ereport
  - errcode
  - errmsg
  - PGSTAT_KIND_FIRST_VALID
  - PGSTAT_KIND_LAST
  - PGSTAT_KIND_DATABASE
  - pgstat_kind_infos (global array)
- Called from (representative examples):
  - pg_stat_have_stats (src/backend/utils/adt/pgstatfuncs.c:2031)

## Notes and Other Information
- Performs case-insensitive string matching using pg_strcasecmp
- Throws an ERROR with ERRCODE_INVALID_PARAMETER_VALUE if the string doesn't match any valid statistics kind
- The return statement after ereport is unreachable but included to satisfy compiler warnings
- Part of the PostgreSQL statistics system's string-to-enum conversion infrastructure
- Used primarily by SQL functions that accept statistics kind names as string parameters