# pgstat_is_kind_valid

## Location
[src/backend/utils/activity/pgstat.c:1259-1264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1259-L1264)

## Overview
A simple inline validation function that checks whether an integer value represents a valid statistics kind within the defined range of PostgreSQL statistics types.

## Definition
static inline bool pgstat_is_kind_valid(int ikind)

## Detailed Description
This inline utility function provides a simple bounds check to determine if an integer value corresponds to a valid PgStat_Kind enumeration value. It performs a range check to ensure the input value falls between PGSTAT_KIND_FIRST_VALID and PGSTAT_KIND_LAST (inclusive). This function is used internally within the statistics system to validate kind values before using them as array indices or in other operations where an invalid kind could cause errors or undefined behavior. Being declared as static inline, it provides efficient validation without function call overhead.

## Parameters / Member Variables
- `ikind`: An integer value to be validated as a statistics kind

## Dependencies
- Functions called/Symbols referenced:
  - PGSTAT_KIND_FIRST_VALID
  - PGSTAT_KIND_LAST
  - [PgStat_KindInfo](../P/PgStat_KindInfo.md)
- Called from (representative examples):
  - [pgstat_snapshot_fixed](pgstat_snapshot_fixed.md) (src/backend/utils/activity/pgstat.c:942)
  - [pgstat_get_kind_info](pgstat_get_kind_info.md) (src/backend/utils/activity/pgstat.c:1267)
  - [pgstat_read_statsfile](pgstat_read_statsfile.md) (src/backend/utils/activity/pgstat.c:1600, 1614)

## Notes and Other Information
- Returns true if the kind value is within the valid range, false otherwise
- Declared as static inline for performance optimization
- Used primarily for input validation and array bounds checking
- Essential for preventing array out-of-bounds access in the pgstat_kind_infos array
- Part of PostgreSQL's defensive programming practices for the statistics subsystem