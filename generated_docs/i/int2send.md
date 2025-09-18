# int2send

## Location
src/backend/utils/adt/int.c: 98 - 113

## Overview
The int2send function converts an internal int2 (16-bit signed integer) value to PostgreSQL's external binary format.

## Definition
```c
Datum int2send(PG_FUNCTION_ARGS)
```

## Detailed Description
int2send is a PostgreSQL send function that handles binary output conversion for the int2 data type. It is the counterpart to int2recv and is part of the binary I/O system that allows PostgreSQL to efficiently transfer data in binary format. The function takes an int2 value and converts it to a binary representation suitable for network transmission. It uses the pq_begintypsend, pq_sendint16, and pq_endtypsend functions to create a properly formatted binary message. This function is typically used in client-server communication when binary protocol is enabled, providing more efficient data transfer compared to text-based protocols.

## Parameters / Member Variables
- `arg1`: The int2 (16-bit signed integer) value to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16
  - pq_begintypsend
  - pq_sendint16
  - pq_endtypsend
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's binary I/O protocol system
- Used for efficient binary data transfer between client and server
- Creates a binary message buffer using StringInfoData
- The binary format follows network byte order conventions
- Returns the binary data as a bytea (binary array) type
- The pq_begintypsend/pq_endtypsend pair handles proper message framing
- The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- Works in conjunction with int2recv for bidirectional binary conversion