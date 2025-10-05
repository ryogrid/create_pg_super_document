# int8send

## Location
[src/backend/utils/adt/int8.c:94-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L94-L112)

## Overview
Converts PostgreSQL's internal int8 (bigint) value to external binary format for transmission.

## Definition

```c
Datum
int8send(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the binary output conversion routine for PostgreSQL's int8 data type (bigint). It takes a 64-bit integer from PostgreSQL's internal Datum representation and converts it to the binary wire protocol format used for client-server communication. This function is part of the PostgreSQL type system's binary input/output infrastructure and is used when sending int8 values in binary format during client-server communication, particularly in prepared statements and binary result sets.

The function creates a StringInfo buffer, initializes it for binary output, writes the 64-bit integer in network byte order, and returns the resulting bytea containing the binary representation.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides access to:
  -  (int64): The 64-bit integer value to convert to binary format

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract int64 argument from function arguments
  - : Initializes StringInfo buffer for binary type sending
  - : Writes 64-bit integer to buffer in network byte order
  - : Finalizes buffer and returns bytea result
  - : Macro to return bytea as Datum
- Called from (representative examples):
  - No direct references found in the current codebase (used internally by the type system)

## Notes and Other Information
- This function is registered in the PostgreSQL type system as the binary send function for the int8/bigint data type
- Used primarily in binary protocol communication between client and server
- Creates a temporary StringInfo buffer to build the binary representation
- Handles network byte order conversion automatically through 
- The resulting bytea contains the binary representation suitable for wire protocol transmission
- Part of the binary I/O functions that enable efficient data transfer without string conversion overhead
- Located in src/backend/utils/adt/int8.c alongside other 64-bit integer utility functions

## Simplified Source

```c
Datum int8send(PG_FUNCTION_ARGS) {
    int64 arg1 = PG_GETARG_INT64(0);  // Get 64-bit integer value
    StringInfoData buf;

    // Initialize binary buffer and write integer
    pq_begintypsend(&buf);
    pq_sendint64(&buf, arg1);

    // Finalize and return binary data
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```