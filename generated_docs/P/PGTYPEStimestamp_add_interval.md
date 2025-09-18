# PGTYPEStimestamp_add_interval

## Location
[src/interfaces/ecpg/pgtypeslib/timestamp.c:862-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/timestamp.c#L862-L916)

## Overview
Adds an interval to a PostgreSQL timestamp, handling both month-based and time-based components of the interval.

## Definition
```c
int PGTYPEStimestamp_add_interval(timestamp *tin, interval *span, timestamp *tout)
```

## Detailed Description
This function performs timestamp arithmetic by adding an interval to a timestamp. It handles the complex logic required for date arithmetic, including month boundaries, leap years, and varying month lengths. For infinite timestamps, it simply copies the input to output. For finite timestamps, it processes month adjustments separately from time adjustments. Month arithmetic involves converting the timestamp to a tm structure, adjusting months and years with proper overflow/underflow handling, checking for end-of-month boundary issues (like adding a month to January 31st), and then converting back to timestamp format. Finally, it adds the time component directly to the timestamp value.

## Parameters / Member Variables
- `tin`: Pointer to the input timestamp to which the interval will be added
- `span`: Pointer to the interval to be added to the timestamp
- `tout`: Pointer to the output timestamp where the result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_NOT_FINITE
  - [timestamp2tm](../t/timestamp2tm.md)
  - [tm2timestamp](../t/tm2timestamp.md)
  - isleap
  - MONTHS_PER_YEAR
  - day_tab
- Called from (representative examples):
  - [PGTYPEStimestamp_sub_interval](PGTYPEStimestamp_sub_interval.md)
  - [main](../m/main.md) (in test cases)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Handles infinite timestamps by copying input to output
- Properly manages month arithmetic with leap year considerations
- Adjusts for end-of-month boundary problems (e.g., February 29 + 1 year)
- Processes month and time components separately for accurate results
- Part of the ECPG pgtypes library for embedded SQL applications