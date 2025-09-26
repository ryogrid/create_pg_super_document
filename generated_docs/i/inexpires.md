# inexpires

## Location
src/timezone/zic.c: 1787 - 1797

## Overview
Processes leap second expiration information from timezone database files, setting when leap second data becomes invalid.

## Definition


## Detailed Description
The  function is part of PostgreSQL's timezone compiler () that handles expiration entries from timezone database files. It processes "Expires" lines that specify when the leap second information in the timezone database becomes outdated and should no longer be trusted.

The function validates that the correct number of fields are present in the expires line, ensures that only one expiration date is specified per timezone database file (multiple expires lines are an error), and parses the expiration datetime using .

The expiration information is stored in the global variable  and is used to determine when leap second data should be considered stale.

## Parameters / Member Variables
- : Array of string fields parsed from the expires line in the timezone database file
- : Number of fields in the fields array, must match EXPIRES_FIELDS for valid expires entries

## Dependencies
- Functions called/Symbols referenced:
  - error (for reporting parsing errors)
  - getleapdatetime (to parse and validate the expiration datetime, with true flag indicating expires context)
  - EXPIRES_FIELDS (constant defining expected number of fields)
  - leapexpires (global variable storing the expiration timestamp)
- Called from (representative examples):
  - infile (main file parsing function)

## Notes and Other Information
- This function is part of PostgreSQL's timezone data compilation system, not the runtime timezone handling
- Only one expires entry is allowed per timezone database file - multiple entries indicate corrupted or malformed data
- The expiration date helps applications determine when they need to update their leap second information
- The  parameter passed to  indicates this is an expiration context rather than a leap second entry
- Leap second data typically has expiration dates to ensure systems don't rely on potentially outdated information indefinitely