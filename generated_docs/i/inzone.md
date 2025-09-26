# inzone

## Location
[src/timezone/zic.c:1518-1555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1518-L1555)

## Overview
Processes a timezone Zone line from input, validates it for conflicts and duplicates, then delegates to inzsub for detailed zone processing.

## Definition

```c
enum);
```
## Detailed Description
The  function validates a timezone Zone line by checking field count, ensuring no conflicts with command-line options (-l and -p), and verifying no duplicate zone names exist. It performs several validation checks:
- Field count must be between ZONE_MINFIELDS (5) and ZONE_MAXFIELDS (9)
- Zone name cannot match tzdefault if -l option is specified
- Zone name cannot be TZDEFRULES ('posixrules') if -p option is specified  
- Zone name must not be a duplicate of existing zones

After validation, it calls  to process the zone data details.

## Parameters / Member Variables
- : Array of string pointers containing the parsed zone fields from input
- : Number of fields provided in the fields array

## Dependencies
- Functions called/Symbols referenced:
  - [error](../e/error.md) (for error reporting)
  - strcmp (string comparison)
  - [inzsub](inzsub.md) (to process zone details)
- Called from (representative examples):
  - [infile](infile.md) (main input processing function)

## Notes and Other Information
- Returns false on validation errors, true on success
- Uses field index ZF_NAME to access the zone name
- Checks against global variables lcltime, tzdefault, and psxrules for option conflicts
- Maintains zones array to track existing zone names and prevent duplicates
- TZDEFRULES is defined as 'posixrules' in tzfile.h
- Part of PostgreSQL's timezone data compilation system (zic)