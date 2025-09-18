# date_timestamp

## Location
[src/backend/utils/adt/date.c:1283-1296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1283-L1296)

## Overview
Converts a PostgreSQL date value to a timestamp data type, adding time component as 00:00:00.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that converts a date value (DateADT) to a timestamp value (Timestamp). This conversion effectively adds a time component of 00:00:00 to the date, creating a timestamp that represents the beginning of that date. The function is implemented as a PostgreSQL V1 calling convention function, taking arguments through the  macro and returning a .

The conversion is performed by calling the internal helper function , which handles the actual conversion logic and potential overflow checking.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure
  - Argument 0:  - The input date value to be converted

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to extract date argument
  -  - Internal helper function for conversion
  -  - Macro to return timestamp result
- Types used:
  -  - PostgreSQL date type
  -  - PostgreSQL timestamp type
  -  - PostgreSQL generic return type
- Called from (representative examples):
  -  (in jsonpath execution)

## Notes and Other Information
- This function is part of PostgreSQL's date/time type conversion system
- The conversion always sets the time component to 00:00:00 (start of day)
- Overflow checking is performed by the underlying  function
- Located in 
- Used primarily in SQL contexts where implicit or explicit conversion from date to timestamp is needed