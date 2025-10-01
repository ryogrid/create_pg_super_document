# AdjustMonths

## Location
[src/backend/utils/adt/datetime.c:649-660](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L649-L660)

## Overview
A static helper function that adds a month value to the months field of a pg_itm_in structure with range and overflow checking.

## Definition
```c
static bool AdjustMonths(int64 val, struct pg_itm_in *itm_in)
```

## Detailed Description
AdjustMonths is a straightforward utility function in PostgreSQL's datetime processing system that adds month values to the tm_mon field of a pg_itm_in structure. Unlike other similar adjustment functions, it does not require a scale parameter because the input value is already in months.

The function implements two levels of safety checking: first ensuring the 64-bit input value can fit within 32-bit integer bounds, and then using PostgreSQL's safe addition function to prevent integer overflow when updating the target field. This simple but robust design makes it reliable for month arithmetic in interval processing.

## Parameters / Member Variables
- `val`: An int64 value representing the number of months to add
- `itm_in`: A pointer to a pg_itm_in structure whose tm_mon field will be modified

## Dependencies
- Functions called/Symbols referenced:
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (for safe 32-bit addition with overflow checking)
  - [pg_itm_in](../p/pg_itm_in.md) (structure type)
- Called from (representative examples):
  - [DecodeInterval](../D/DecodeInterval.md) (for processing month components in interval parsing)
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (for ISO 8601 interval parsing month handling)

## Notes and Other Information
- Returns true on success, false if range check fails or overflow occurs
- No scaling is performed as the input value is already in months
- Simpler than other Adjust functions due to direct month-to-month mapping
- Part of PostgreSQL's suite of overflow-safe datetime arithmetic functions
- Located at src/backend/utils/adt/datetime.c:649-660
- The comment explicitly notes that no scaling is needed since val is already in months

## Simplified Source

```c
static bool AdjustMonths(int64 val, struct pg_itm_in *itm_in) {
    // Check if 64-bit value fits in 32-bit range
    if (val < INT_MIN || val > INT_MAX)
        return false;

    // Safely add months to existing count (no scaling needed)
    return !pg_add_s32_overflow(itm_in->tm_mon, (int32) val, &itm_in->tm_mon);
}
```