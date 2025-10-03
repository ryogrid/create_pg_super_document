# AddPostgresIntPart

## Location
[src/backend/utils/adt/datetime.c:4523-4545](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4523-L4545)

## Overview
A static helper function that appends a PostgreSQL-style interval field to a string buffer, but only if the value is non-zero.

## Definition

```c
static char *
AddPostgresIntPart(char *cp, int64 value, const char *units,
				   bool *is_zero, bool *is_before)
```
## Detailed Description
This function is responsible for formatting individual components of PostgreSQL interval values in the traditional PostgreSQL interval output format. It conditionally appends interval field values (like years, months, days, etc.) to a string buffer only when the value is non-zero. The function handles proper spacing, sign formatting, and pluralization of unit names. It implements a specific behavior where each nonzero field influences the sign formatting of the subsequent field through the  parameter.

## Parameters / Member Variables
- `*cp`: Pointer to the current position in the output string buffer where the formatted interval part should be appended
- `value`: The numeric value of the interval component (e.g., number of years, months, days)
- `*units`: The unit name string (e.g., "year", "month", "day")
- `*is_zero`: Pointer to a boolean flag indicating whether any non-zero values have been encountered yet
- `*is_before`: Pointer to a boolean flag that tracks sign state for proper formatting of subsequent fields
## Dependencies
- Functions called/Symbols referenced:
  - sprintf (standard C library function)
  - strlen (standard C library function)
- Called from (representative examples):
  - [EncodeInterval](../E/EncodeInterval.md) (in src/backend/utils/adt/datetime.c)
  - [EncodeInterval](../E/EncodeInterval.md) (in src/interfaces/ecpg/pgtypeslib/interval.c)

## Notes and Other Information
- The function only appends content when the value is non-zero, which helps create clean interval representations
- Handles pluralization automatically by appending 's' to unit names when value != 1
- Implements a specific PostgreSQL formatting behavior where negative values in one field affect the sign display of positive values in the next field
- Returns an updated pointer to the end of the newly written content for easy chaining of multiple interval parts
- Part of PostgreSQL's interval data type formatting system, specifically for the traditional PostgreSQL output style

## Simplified Source

```c
static char *
AddPostgresIntPart(char *cp, int64 value, const char *units,
                   bool *is_zero, bool *is_before)
{
    // Skip zero values
    if (value == 0)
        return cp;

    // Format: [space] [+] value units[s]
    sprintf(cp, "%s%s%lld %s%s",
            (!*is_zero) ? " " : "",                    // Space separator (except first field)
            (*is_before && value > 0) ? "+" : "",      // Plus sign for positive after negative
            (long long) value,                         // The numeric value
            units,                                     // Unit name ("year", "month", etc.)
            (value != 1) ? "s" : "");                  // Pluralize unless exactly 1

    // Update state for next field formatting
    *is_before = (value < 0);  // Next field gets "+" if this was negative
    *is_zero = false;          // No longer the first field

    // Return pointer to end for chaining
    return cp + strlen(cp);
}
```