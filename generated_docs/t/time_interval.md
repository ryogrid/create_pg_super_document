# time_interval

## Location
src/backend/utils/adt/date.c: 1989 - 2011

## Overview
Converts a time value to an interval data type, creating an interval that represents the same duration as the time value from midnight.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that converts a TimeADT (time) value into an Interval data type. The resulting interval represents the same duration as the time value, measured from midnight (00:00:00). The function creates a new Interval structure with the time component set to the input time value, while the day and month components are set to zero, indicating that this interval represents only a time duration without any date-based components.

The function performs the following operations:
1. Extracts the time argument from the function parameters
2. Allocates memory for a new Interval structure using 
3. Sets the time field of the interval to the input time value
4. Initializes the day and month fields to zero
5. Returns the newly created interval

This conversion is useful for treating time values as durations in interval arithmetic operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (TimeADT): The time value to convert to an interval

## Dependencies
- Functions called/Symbols referenced:
  - TimeADT: Time abstract data type for input time values
  - PG_GETARG_TIMEADT: Macro to extract TimeADT argument
  - Interval: Interval data structure for storing time periods
  - palloc: PostgreSQL memory allocation function
  - PG_RETURN_INTERVAL_P: Macro to return Interval pointer result

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- The resulting interval has zero days and months, representing only the time component
- Memory is dynamically allocated for the Interval result using PostgreSQL's memory management
- The time field in the interval stores the duration in microseconds from midnight
- This conversion allows time values to participate in interval arithmetic operations
- The function creates a "pure time" interval without any calendar-based components
- Located in src/backend/utils/adt/date.c:1989-2011