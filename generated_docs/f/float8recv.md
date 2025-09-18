# float8recv

## Location
src/backend/utils/adt/float.c: 549 - 559

## Overview
PostgreSQL system function that converts float8 values from external binary format to internal representation for use in binary protocol communication.

## Definition


## Detailed Description
This function serves as the PostgreSQL system interface for receiving float8 values in binary format over the network protocol. It handles the conversion from the PostgreSQL binary wire protocol format to the internal float8 representation. This function is part of the binary I/O infrastructure used when clients communicate with PostgreSQL using the binary protocol (as opposed to text protocol).

The function extracts a StringInfo buffer containing the binary data and uses the protocol message parsing infrastructure to read an 8-byte IEEE 754 double-precision floating-point value.

## Parameters / Member Variables
- Uses  macro which provides access to function arguments through the PostgreSQL function call interface
- Extracts one StringInfo argument using  representing the binary input buffer

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (macro to extract pointer argument)
  - pq_getmsgfloat8 (protocol function to read 8-byte float from message buffer)
  - PG_RETURN_FLOAT8 (macro to return float8 as Datum)

- Called from (representative examples):
  - System catalog functions (registered as receive function for float8 type)
  - No direct references found in indexed code

## Notes and Other Information
- Part of PostgreSQL's binary protocol infrastructure
- Registered in system catalogs as the receive function for float8 data type
- Used automatically when clients send float8 values using binary protocol
- Handles network byte order conversion if necessary (handled by pq_getmsgfloat8)
- Complementary to float8send() for binary protocol communication
- Enables efficient transfer of float8 values without text conversion overhead