# time_send

## Location
src/backend/utils/adt/date.c: 1547 - 1557

## Overview
Converts PostgreSQL's internal TimeADT representation to external binary format for network transmission and storage.

## Definition


## Detailed Description
The  function is a PostgreSQL output function that serializes time values to their binary external representation. It takes a TimeADT value (representing microseconds since midnight) and converts it to a bytea value containing the binary representation in network byte order. This function is part of PostgreSQL's binary I/O system for the time data type and is the complement to .

The function uses PostgreSQL's standard binary output protocol, creating a StringInfo buffer, writing the 64-bit time value, and returning the resulting bytea.

## Parameters / Member Variables
-  (TimeADT): The internal time value to be serialized, representing microseconds since midnight

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts TimeADT argument from function call
  - : Initializes binary output buffer
  - : Writes 64-bit integer to output buffer in network byte order  
  - : Finalizes binary output buffer
  - : Returns the binary data as a bytea Datum
- Types used:
  - : Internal representation of time values
  - : Buffer for constructing binary output

## Notes and Other Information
- This is a binary output function, complementary to  for binary input
- The output format is a 64-bit integer in network byte order representing microseconds since midnight
- Used by PostgreSQL's binary protocol for efficient data transfer
- Part of the time data type's I/O function suite
- Located in src/backend/utils/adt/date.c:1547-1557