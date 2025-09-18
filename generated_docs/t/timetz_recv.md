# timetz_recv

## Location
src/backend/utils/adt/date.c: 2335 - 2370

## Overview
The timetz_recv function is PostgreSQL's binary receive function for the TIME WITH TIME ZONE data type, responsible for converting external binary format data into the internal TimeTzADT representation.

## Definition
```c
Datum timetz_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the binary input handler for TIME WITH TIME ZONE values in PostgreSQL's binary protocol. It reads binary data from a StringInfo buffer and constructs a TimeTzADT structure, performing validation to ensure the data is within acceptable ranges.

The function performs these key operations:
1. Allocates memory for a new TimeTzADT structure
2. Reads the time value (microseconds since midnight) as a 64-bit integer from the binary buffer
3. Validates that the time value is within the valid range (0 to USECS_PER_DAY)
4. Reads the timezone offset as a signed integer from the binary buffer
5. Validates that the timezone offset is within acceptable displacement limits (±TZDISP_LIMIT)
6. Applies any type modifier constraints using AdjustTimeForTypmod
7. Returns the constructed TimeTzADT value

The function includes comprehensive range checking to prevent invalid time and timezone values from being accepted.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the binary data to be parsed
- `typelem`: Type element OID (currently unused, marked with NOT_USED)
- `typmod`: Type modifier specifying precision constraints for the time value

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint64](../p/pq_getmsgint64.md)
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md)
  - PG_RETURN_TIMETZADT_P
  - [palloc](../p/palloc.md)
  - ereport (for error reporting)
- Called from (representative examples):
  - No direct callers found (typically invoked through PostgreSQL's type system during binary protocol handling)

## Notes and Other Information
- This is the binary receive function for the TIME WITH TIME ZONE data type in PostgreSQL
- Located in src/backend/utils/adt/date.c as part of the date/time ADT implementations
- Used primarily in PostgreSQL's binary wire protocol (e.g., when using prepared statements with binary parameters)
- Performs strict validation of both time and timezone components to prevent invalid data
- Time values must be between 0 and USECS_PER_DAY (24 hours worth of microseconds)
- Timezone displacements must be within ±TZDISP_LIMIT (typically ±15 hours)
- Memory for the result is allocated using palloc, managed by PostgreSQL's memory context system
- Errors are reported using ereport with appropriate error codes for out-of-range conditions
- Type modifiers are applied to enforce precision constraints on the time component