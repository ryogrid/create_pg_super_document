# init_ttinfo

## Location
[src/timezone/localtime.c:108-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L108-L117)

## Overview
Initializes a timezone transition type information structure with specified UTC offset, DST status, and time zone abbreviation index.

## Definition

```c
static void
init_ttinfo(struct ttinfo *s, int32 utoff, bool isdst, int desigidx)
```
## Detailed Description
The  function is a utility function used in PostgreSQL's timezone handling system to initialize a  object with basic timezone transition information. It sets the primary timezone properties while initializing the standard time and UT flags to false, which can be adjusted later if needed.

This function is part of the timezone parsing and management system, specifically used when creating timezone transition rules from timezone data files or POSIX timezone strings.

## Parameters / Member Variables
- : Pointer to the  to be initialized
- : UTC offset in seconds for this timezone transition type
- : Boolean flag indicating whether this represents daylight saving time
- : Index into the timezone abbreviation list for this transition type

## Dependencies
- Functions called/Symbols referenced:
  - ttinfo (struct type)
- Called from (representative examples):
  - tzparse (multiple calls at lines 1053, 1054, 1217, 1218, 1228)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c compilation unit
- The function always initializes  and  to false, which are flags indicating whether the transition time is in standard time or universal time
- Used primarily during timezone rule parsing to create transition type records
- The function provides a clean initialization pattern for ttinfo structures, ensuring all fields are properly set