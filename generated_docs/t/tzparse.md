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
  - getqzname, getzname (for parsing timezone names)
  - getoffset (for parsing timezone offsets)
  - getrule (for parsing transition rules)
  - transtime (for calculating actual transition times)
  - init_ttinfo (for initializing timezone type information)
  - increment_overflow_time (for safe time arithmetic)
  - isleap (for leap year calculations)
  - EPOCH_YEAR, YEARSPERREPEAT, TZ_MAX_TIMES, SECSPERHOUR, SECSPERDAY (constants)
  - TZDEFRULESTRING (default transition rules)
- Called from (representative examples):
  - pg_load_tz (from initdb)
  - tzloadbody
  - gmtload
  - pg_tzset
  - pg_tz

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