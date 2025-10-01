# adjust_partial_year_to_2020

## Location
[src/backend/utils/adt/formatting.c:2354-2377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2354-L2377)

## Overview
Adjusts partial year values to full 4-digit years using a pivot approach centered around the year 2020.

## Definition
```c
static int adjust_partial_year_to_2020(int year)
```

## Detailed Description
This function implements a year adjustment algorithm that converts partial year representations (1-3 digits) into full 4-digit years. The algorithm is designed with a pivot around the year 2020, making reasonable assumptions about which century a partial year should represent. This is particularly important for date parsing where users might input abbreviated years like '98' (meaning 1998) or '05' (meaning 2005). The function handles various ranges systematically to provide intuitive year interpretations.

## Parameters / Member Variables
- `year`: The input year value, which may be a partial year (1-3 digits) or already a full year

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic arithmetic operations)
- Called from (representative examples):
  - DCH_ZONED
  - [DCH_from_char](../D/DCH_from_char.md)

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/formatting.c
- The algorithm uses the following rules:
  - Years 0-69: Interpreted as 2000-2069 (21st century)
  - Years 70-99: Interpreted as 1970-1999 (20th century)
  - Years 100-519: Interpreted as 2100-2519 (22nd-26th centuries)
  - Years 520-999: Interpreted as 1520-1999 (16th-20th centuries)
  - Years ≥1000: Returned unchanged (already full years)
- The pivot point of 70 follows common conventions where '70' represents 1970 and '69' represents 2069
- Used primarily in date/time parsing functions for handling abbreviated year inputs
- Critical for maintaining backward compatibility with legacy date formats while providing sensible defaults for future dates

## Simplified Source

```c
static int
adjust_partial_year_to_2020(int year)
{
    // Adjust partial years toward 2020 for intuitive interpretation

    if (year < 70)          // 0-69 -> 2000-2069 (near future/recent past)
        return year + 2000;
    else if (year < 100)    // 70-99 -> 1970-1999 (20th century)
        return year + 1900;
    else if (year < 520)    // 100-519 -> 2100-2519 (far future)
        return year + 2000;
    else if (year < 1000)   // 520-999 -> 1520-1999 (distant past)
        return year + 1000;
    else                    // >=1000 -> already full year
        return year;
}
```