# int2recv

## Location
src/backend/utils/adt/int.c: 87 - 97

## Overview
The int2recv function converts data from PostgreSQL's external binary format to an internal int2 (16-bit signed integer) value.

## Definition
```c
Datum int2recv(PG_FUNCTION_ARGS)
```

## Detailed Description
int2recv is a PostgreSQL receive function that handles binary input conversion for the int2 data type. It is part of the binary I/O system that allows PostgreSQL to efficiently transfer data in binary format rather than text format. The function reads binary data from a StringInfo buffer and converts it to the internal int2 representation. This function is typically used in client-server communication when binary protocol is enabled, providing more efficient data transfer compared to text-based protocols. The function uses pq_getmsgint to safely extract a 16-bit integer from the binary message buffer.

## Parameters / Member Variables
- `buf`: StringInfo pointer containing the binary data buffer to read from

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint
  - PG_RETURN_INT16
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's binary I/O protocol system
- Used for efficient binary data transfer between client and server
- Reads exactly sizeof(int16) bytes from the input buffer
- The binary format follows network byte order conventions
- Error handling for malformed binary data is handled by pq_getmsgint
- The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- Returns a Datum containing the converted int2 value