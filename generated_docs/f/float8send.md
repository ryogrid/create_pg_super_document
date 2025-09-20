# float8send

## Location
[src/backend/utils/adt/float.c:560-583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L560-L583)

## Overview
PostgreSQL system function that converts float8 values to binary format for transmission over the network protocol in client-server communication.

## Definition

```c
Datum
float8send(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the PostgreSQL system interface for sending float8 values in binary format over the network protocol. It handles the conversion from internal float8 representation to the PostgreSQL binary wire protocol format. This function is part of the binary I/O infrastructure used when clients communicate with PostgreSQL using the binary protocol.

The function creates a StringInfo buffer, writes the float8 value in network byte order format, and returns the resulting binary data as a bytea value. This enables efficient transmission of floating-point values without the overhead and precision loss of text conversion.

## Parameters / Member Variables
- Uses  macro which provides access to function arguments through the PostgreSQL function call interface
- Extracts one float8 argument using 

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - [pq_begintypsend](../p/pq_begintypsend.md) (initialize binary output buffer)
  - [pq_sendfloat8](../p/pq_sendfloat8.md) (write 8-byte float to message buffer with proper byte order)
  - [pq_endtypsend](../p/pq_endtypsend.md) (finalize binary output buffer)
  - PG_RETURN_BYTEA_P (macro to return binary data as bytea Datum)

- Called from (representative examples):
  - System catalog functions (registered as send function for float8 type)
  - No direct references found in indexed code

## Notes and Other Information
- Part of PostgreSQL's binary protocol infrastructure
- Registered in system catalogs as the send function for float8 data type
- Used automatically when clients request float8 values using binary protocol
- Handles network byte order conversion (handled by pq_sendfloat8)
- Complementary to float8recv() for binary protocol communication
- Returns bytea format suitable for network transmission
- Enables efficient transfer of float8 values without text conversion overhead
- Used by PostgreSQL client libraries when binary protocol is enabled