# j2date

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:606-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L606-L634)

## Overview
Converts a Julian day number back to calendar date components (year, month, day), serving as the inverse function to date2j().

## Definition

```c
void
j2date(int jd, int *year, int *month, int *day)
```
## Detailed Description
j2date implements the inverse of the date2j() function, converting a Julian day number back to its corresponding calendar date components. The algorithm uses a series of mathematical operations involving division, modulo arithmetic, and carefully chosen constants to decompose the Julian day number into year, month, and day values.

The function employs unsigned integer arithmetic to handle the complex calculations required for the reverse Julian-to-calendar conversion. It accounts for leap years, varying month lengths, and the Gregorian calendar system through a sophisticated algorithm that uses quad-year cycles and other astronomical constants. The implementation handles the same range as date2j(), working correctly for Julian day numbers corresponding to dates from Nov 24, -4713 onwards.

## Parameters / Member Variables
- : The Julian day number to convert to calendar date
- : Output parameter that receives the calendar year
- : Output parameter that receives the month (1-12)
- : Output parameter that receives the day of the month (1-31)

## Dependencies
- Functions called/Symbols referenced:
  - MONTHS_PER_YEAR (constant defining 12 months per year)
- Called from (representative examples):
  - [date_out](../d/date_out.md) (date output formatting)
  - [DecodeDateTime](../D/DecodeDateTime.md) (date/time parsing operations)
  - [timestamp2tm](../t/timestamp2tm.md) (timestamp to tm structure conversion)
  - [extract_date](../e/extract_date.md) (date component extraction)
  - [isoweek2date](../i/isoweek2date.md) (ISO week to date conversion)
  - [ValidateDate](../V/ValidateDate.md) (date validation)

## Notes and Other Information
- Function returns void and modifies the output parameters passed by reference
- Uses unsigned integer arithmetic for intermediate calculations to avoid overflow issues
- The algorithm involves several magic numbers (32044, 146097, 1461, etc.) derived from astronomical and calendar calculations
- Central to PostgreSQL's date output and conversion operations
- Works as the exact inverse of date2j() - applying both functions in sequence returns the original values
- Essential for displaying dates to users and converting internal Julian representations back to human-readable format
- The complex arithmetic handles leap year cycles, century adjustments, and month/day calculations in a mathematically efficient manner

## Simplified Source

```c
// Simplified version of j2date
void j2date(int jd, int *year, int *month, int *day)
{
    unsigned int julian;
    unsigned int quad;
    unsigned int extra;
    int y;

    // Step 1: Adjust Julian day and compute century cycles
    julian = jd + 32044;  // Adjust for epoch
    quad = julian / 146097;  // 400-year cycles
    extra = (julian - quad * 146097) * 4 + 3;

    // Step 2: Further adjust for leap year calculations
    julian += 60 + quad * 3 + extra / 146097;

    // Step 3: Compute 4-year cycles and extract year
    quad = julian / 1461;  // 4-year cycles
    julian -= quad * 1461;
    y = julian * 4 / 1461;

    // Step 4: Calculate day within year, accounting for leap years
    if (y != 0) {
        julian = (julian + 305) % 365;
    } else {
        julian = (julian + 306) % 366;  // Leap year adjustment
    }
    julian += 123;

    // Step 5: Combine cycles to get final year
    y += quad * 4;
    *year = y - 4800;  // Adjust from astronomical year to calendar year

    // Step 6: Extract month and day from day-of-year
    quad = julian * 2141 / 65536;  // Month calculation
    *day = julian - 7834 * quad / 256;  // Day calculation
    *month = (quad + 10) % MONTHS_PER_YEAR + 1;  // Convert to 1-12 range
}
```

Key simplifications made:
- Added descriptive comments explaining each major calculation step
- Preserved all original mathematical operations as they're essential for correctness
- Clarified the purpose of magic numbers through comments
- Organized the algorithm into logical steps for better readability
- Maintained the exact algorithmic structure since this is a precision mathematical function