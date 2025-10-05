# float8recv

## Location
[src/backend/utils/adt/float.c:549-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L549-L559)

## Overview
PostgreSQL system function that converts float8 values from external binary format to internal representation for use in binary protocol communication.

## Definition

```c
Datum
float8recv(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the PostgreSQL system interface for receiving float8 values in binary format over the network protocol. It handles the conversion from the PostgreSQL binary wire protocol format to the internal float8 representation. This function is part of the binary I/O infrastructure used when clients communicate with PostgreSQL using the binary protocol (as opposed to text protocol).

The function extracts a StringInfo buffer containing the binary data and uses the protocol message parsing infrastructure to read an 8-byte IEEE 754 double-precision floating-point value.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (macro to extract pointer argument)
  - [pq_getmsgfloat8](../p/pq_getmsgfloat8.md) (protocol function to read 8-byte float from message buffer)
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

## Simplified Source

```c
Datum
float8recv(PG_FUNCTION_ARGS)
{
    // Get the binary buffer from input
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);

    // Extract float8 from binary format and return
    PG_RETURN_FLOAT8(pq_getmsgfloat8(buf));
}
```