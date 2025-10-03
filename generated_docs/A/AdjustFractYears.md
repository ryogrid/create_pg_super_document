# AdjustFractYears

## Location
[src/backend/utils/adt/datetime.c:601-617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L601-L617)

## Overview
A static helper function that converts fractional years to months and adds them to the months field of a pg_itm_in structure, handling potential overflow.

## Definition

```c
static bool
AdjustFractYears(double frac, int scale,
				 struct pg_itm_in *itm_in)
```
## Detailed Description
AdjustFractYears is a utility function used in PostgreSQL's datetime parsing and interval processing. It takes a fractional value representing a portion of years, multiplies it by a scale factor to produce years, then converts those years to months by multiplying by MONTHS_PER_YEAR (12). The resulting integral number of months is added to the tm_mon field of the input pg_itm_in structure.

The function performs safe integer arithmetic using PostgreSQL's overflow-checking addition function to prevent integer overflow. It assumes that the absolute value of the fraction is less than 1, which ensures that the multiplication operations cannot overflow for any reasonable scale value.

## Parameters / Member Variables
- `frac`: A double representing the fractional years value (assumed to have absolute value < 1)
- `scale`: An integer scale factor used to convert the fraction to years
- `*itm_in`: A pointer to a pg_itm_in structure whose tm_mon field will be modified
## Dependencies
- Functions called/Symbols referenced:
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (for safe integer addition)
  - MONTHS_PER_YEAR (constant, value 12)
  - [pg_itm_in](../p/pg_itm_in.md) (structure type)
- Called from (representative examples):
  - [DecodeInterval](../D/DecodeInterval.md) (multiple times for different time unit processing)
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (for ISO 8601 interval parsing)

## Notes and Other Information
- The function returns true on success, false if integer overflow occurs
- Uses rint() for proper rounding of the fractional result to the nearest integer
- Part of PostgreSQL's datetime/interval parsing infrastructure in datetime.c
- The overflow check ensures robustness when dealing with extreme input values
- Located at src/backend/utils/adt/datetime.c:601-617

## Simplified Source

```c
static bool AdjustFractYears(double frac, int scale, struct pg_itm_in *itm_in) {
    // Convert fractional years to months with proper rounding
    // frac * scale = years, then * 12 = months
    int extra_months = (int) rint(frac * scale * MONTHS_PER_YEAR);

    // Safely add months to existing count
    return !pg_add_s32_overflow(itm_in->tm_mon, extra_months, &itm_in->tm_mon);
}
```