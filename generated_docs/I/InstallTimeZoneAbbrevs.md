# InstallTimeZoneAbbrevs

## Location
[src/backend/utils/adt/datetime.c:4957-4969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4957-L4969)

## Overview
InstallTimeZoneAbbrevs atomically installs a new TimeZoneAbbrevTable as the active timezone abbreviation lookup table for the PostgreSQL server.

## Definition

```c
void
InstallTimeZoneAbbrevs(TimeZoneAbbrevTable *tbl)
```
## Detailed Description
This function serves as the final step in timezone abbreviation table installation, making a newly constructed TimeZoneAbbrevTable active for use by all timestamp parsing operations. The function performs two critical operations:

1. **Table installation**: Updates the global zoneabbrevtbl pointer to reference the new table
2. **Cache invalidation**: Clears the abbrevcache array to ensure stale pointers from the previous table don't cause memory access errors

The function is designed to be atomic - once called, all subsequent timezone abbreviation lookups will use the new table. This is essential for proper handling of configuration reloads where the timezone_abbreviations GUC parameter changes.

The caller bears responsibility for ensuring the table's memory remains valid for the lifetime of its use, typically through GUC memory management.

## Parameters / Member Variables

.if !dTS .ds TS
.if !dTE .ds TE
.lf 1 -: Pointer to the TimeZoneAbbrevTable to install as the active timezone abbreviation table

## Dependencies
- Functions called/Symbols referenced:
  - memset (clear memory contents)
- Global variables accessed:
  - zoneabbrevtbl (global timezone abbreviation table pointer)
  - abbrevcache (global abbreviation lookup cache array)
- Called from (representative examples):
  - [assign_timezone_abbreviations](../a/assign_timezone_abbreviations.md) (src/backend/commands/variable.c:523)

## Notes and Other Information
- This function is typically called from GUC assignment hooks when the timezone_abbreviations configuration parameter is changed
- The abbrevcache clearing is critical for memory safety - the cache may contain pointers into the previous table's memory
- No validation is performed on the input table - the caller must ensure it's properly constructed
- The function assumes single-threaded access during configuration changes
- Memory management of the old table (if any) is the caller's responsibility
- Part of PostgreSQL's timezone configuration management subsystem

## Simplified Source

```c
void InstallTimeZoneAbbrevs(TimeZoneAbbrevTable *tbl) {
    // Install new timezone abbreviation table
    zoneabbrevtbl = tbl;

    // Clear cache to prevent stale pointers to old table
    memset(abbrevcache, 0, sizeof(abbrevcache));
}
```