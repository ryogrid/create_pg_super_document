# timestamptz_out

## Location
src/backend/utils/adt/timestamp.c: 785 - 812

## Overview
Converts a timestamptz (timestamp with time zone) value to its external string representation for output purposes.

## Definition


## Detailed Description
The  function is responsible for converting PostgreSQL's internal timestamptz representation to a human-readable string format. This function handles both finite and infinite timestamp values, properly formatting them according to the current DateStyle setting while including timezone information. It serves as the output function for the timestamptz data type, called whenever a timestamptz value needs to be displayed or exported.

The function follows PostgreSQL's standard input/output function convention, taking arguments through the  macro and returning a  containing a C string representation of the timestamp.

## Parameters / Member Variables
- Input: A timestamptz value obtained through 
- Output: A  containing a C string representation of the timestamp

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract timestamptz argument
  - : Macro to check for infinite timestamps
  - : Handles encoding of infinite timestamp values
  - : Converts timestamp to broken-down time structure
  - : Formats the timestamp according to DateStyle settings
  - : Creates a palloc'd copy of the formatted string
  - : Macro to return the C string result
- Called from (representative examples):
  - : Used during JSON value extraction

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:785-812
- Handles special timestamp values (infinity, -infinity) through 
- Uses timezone information from  to include proper timezone representation
- Returns a palloc'd string that must be freed by the caller
- Throws an error if the timestamp value is out of range
- The output format depends on the current  setting