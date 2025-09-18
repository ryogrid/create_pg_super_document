# ClearPgItmIn

## Location
[src/backend/utils/adt/datetime.c:3340-3363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L3340-L3363)

## Overview
Initializes a pg_itm_in structure by zeroing out all its time component fields.

## Definition
```c
static inline void ClearPgItmIn(struct pg_itm_in *itm_in)
```

## Detailed Description
This is a simple utility function that initializes a pg_itm_in structure to a clean state by setting all time-related fields to zero. The function is declared as static inline for efficient execution and is used internally within the datetime processing module. It ensures that interval parsing operations start with a known, clean state.

## Parameters / Member Variables
- `itm_in`: Pointer to a pg_itm_in structure to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - pg_itm_in
- Called from (representative examples):
  - [DecodeInterval](../D/DecodeInterval.md)
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the datetime.c compilation unit
- The function clears tm_usec (microseconds), tm_mday (day), tm_mon (month), and tm_year (year) fields
- Used as initialization step before parsing interval strings to ensure consistent starting state
- The pg_itm_in structure represents interval time components for PostgreSQL's internal time processing