# unknownsend

## Location
src/backend/utils/adt/varlena.c: 673 - 692

## Overview
Converts PostgreSQL's internal representation of an unknown data type to external binary format for network transmission.

## Definition
```c
Datum unknownsend(PG_FUNCTION_ARGS)
```

## Detailed Description
The `unknownsend` function is a binary output function for PostgreSQL's unknown data type. It takes the internal C string representation of an unknown value and converts it into a binary format suitable for transmission over network protocols (such as the PostgreSQL wire protocol). The function uses PostgreSQL's standard binary serialization functions to create a properly formatted binary message.

This function complements `unknownrecv` as part of PostgreSQL's binary protocol support, enabling unknown type values to be transmitted efficiently between client and server.

## Parameters / Member Variables
- Input: A C string representing the internal form of an unknown value (accessed via PG_GETARG_CSTRING(0))
- Return: A bytea containing the binary-encoded value (returned via PG_RETURN_BYTEA_P)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting C string argument)
  - [pq_begintypsend](../p/pq_begintypsend.md) (function to initialize binary output buffer)
  - pq_sendtext (function to append text data to binary buffer)
  - strlen (standard C function to get string length)
  - [pq_endtypsend](../p/pq_endtypsend.md) (function to finalize binary output buffer)
  - PG_RETURN_BYTEA_P (macro for returning binary data)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is located in src/backend/utils/adt/varlena.c at lines 673-692
- Uses the standard PostgreSQL binary protocol serialization pattern: begintypsend → sendtext → endtypsend
- The resulting binary format includes protocol headers and length information
- Forms a complementary pair with unknownrecv for complete binary protocol support
- Memory management is handled automatically by the pq_* functions
- The binary format is compatible with PostgreSQL's wire protocol specifications