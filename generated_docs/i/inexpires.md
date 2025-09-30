# inexpires

## Location
[src/timezone/zic.c:1787-1797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1787-L1797)

## Overview
Processes leap second expiration information from timezone database files, setting when leap second data becomes invalid.

## Definition

```c
struct link l;
```
## Detailed Description
The  function is part of PostgreSQL's timezone compiler () that handles expiration entries from timezone database files. It processes "Expires" lines that specify when the leap second information in the timezone database becomes outdated and should no longer be trusted.

The function validates that the correct number of fields are present in the expires line, ensures that only one expiration date is specified per timezone database file (multiple expires lines are an error), and parses the expiration datetime using .

The expiration information is stored in the global variable  and is used to determine when leap second data should be considered stale.

## Parameters / Member Variables
- : Array of string fields parsed from the expires line in the timezone database file
- : Number of fields in the fields array, must match EXPIRES_FIELDS for valid expires entries

## Dependencies
- Functions called/Symbols referenced:
  - [error](../e/error.md) (for reporting parsing errors)
  - [getleapdatetime](../g/getleapdatetime.md) (to parse and validate the expiration datetime, with true flag indicating expires context)
  - EXPIRES_FIELDS (constant defining expected number of fields)
  - leapexpires (global variable storing the expiration timestamp)
- Called from (representative examples):
  - [infile](infile.md) (main file parsing function)

## Notes and Other Information
- This function is part of PostgreSQL's timezone data compilation system, not the runtime timezone handling
- Only one expires entry is allowed per timezone database file - multiple entries indicate corrupted or malformed data
- The expiration date helps applications determine when they need to update their leap second information
- The  parameter passed to  indicates this is an expiration context rather than a leap second entry
- Leap second data typically has expiration dates to ensure systems don't rely on potentially outdated information indefinitely

## Simplified Source

```c
static void inexpires(char **fields, int nfields) {
    // Validate field count
    if (nfields != EXPIRES_FIELDS)
        error("wrong number of fields on Expires line");
    // Check for duplicate expires lines
    else if (0 <= leapexpires)
        error("multiple Expires lines");
    // Parse and store expiration datetime
    else
        leapexpires = getleapdatetime(fields, nfields, true);
}
```