# time_mi_time

## Location
[src/backend/utils/adt/date.c:2033-2051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2033-L2051)

## Overview
Subtracts two time values to produce an interval representing the difference between them.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that performs subtraction between two TimeADT (time) values and returns the result as an Interval. The function calculates the temporal difference between the two input times and creates an interval representing this duration. The resulting interval contains only a time component (in microseconds), with the day and month fields set to zero.

The function performs straightforward arithmetic subtraction between the two time values, which are represented internally as microseconds since midnight. The result can be positive (when time1 > time2) or negative (when time1 < time2), allowing for proper representation of temporal differences in either direction.

The function performs the following operations:
1. Extracts both time arguments from the function parameters
2. Allocates memory for a new Interval structure using 
3. Sets the month and day fields to zero (since this is a time-only difference)
4. Calculates the time difference by subtracting time2 from time1
5. Returns the newly created interval containing the time difference

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (TimeADT): The first time value (minuend)
  -  (TimeADT): The second time value (subtrahend) to subtract from time1

## Dependencies
- Functions called/Symbols referenced:
  - TimeADT: Time abstract data type for input time values
  - PG_GETARG_TIMEADT: Macro to extract TimeADT arguments
  - Interval: Interval data structure for storing the result
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - PG_RETURN_INTERVAL_P: Macro to return Interval pointer result

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- The result represents the difference time1 - time2 as an interval
- Positive results indicate time1 is later than time2, negative results indicate time1 is earlier
- The interval result has zero days and months, containing only the time component
- Memory is dynamically allocated for the Interval result using PostgreSQL's memory management
- The operation is performed in microseconds precision
- Cross-midnight calculations will produce appropriate positive or negative intervals
- Located in src/backend/utils/adt/date.c:2033-2051