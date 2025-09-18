# time_recv

## Location
src/backend/utils/adt/date.c: 1521 - 1546

## Overview
Converts external binary format to PostgreSQL's internal TimeADT representation, handling deserialization of time values from network byte order.

## Definition


## Detailed Description
The  function is a PostgreSQL input function that deserializes time values from their binary external representation. It reads a 64-bit integer representing microseconds since midnight from a StringInfo buffer, validates the range, and applies any necessary type modifier adjustments. This function is part of PostgreSQL's binary I/O system for the time data type.

The function performs strict range validation to ensure the time value falls within the valid range of 0 to USECS_PER_DAY (microseconds in a day). If the value is out of range, it raises a DATETIME_VALUE_OUT_OF_RANGE error.

## Parameters / Member Variables
-  (StringInfo): Input buffer containing the binary time data in network byte order
-  (Oid): Type element OID (currently unused, marked with NOT_USED)
-  (int32): Type modifier specifying precision constraints for the time value

## Dependencies
- Functions called/Symbols referenced:
  - : Reads 64-bit integer from message buffer
  - : Applies type modifier constraints to the time value
  - : Returns the TimeADT value as a Datum
- Constants used:
  - : 64-bit constant for range checking
  - : Maximum valid microseconds in a day
- Types used:
  - : Internal representation of time values

## Notes and Other Information
- This is a binary input function, complementary to  for binary output
- The function assumes input is in microseconds since midnight format
- Range validation ensures time values are within a single day (0 ≤ time < USECS_PER_DAY)
- Type modifier adjustments handle precision constraints (e.g., TIME(3) for 3 decimal places)
- Located in src/backend/utils/adt/date.c:1521-1546