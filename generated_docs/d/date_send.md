# date_send

## Location
src/backend/utils/adt/date.c: 231 - 244

## Overview
Converts PostgreSQL date values from the internal DateADT representation to external binary format for transmission to clients.

## Definition
```c
Datum date_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `date_send` function is responsible for converting date values from PostgreSQL's internal DateADT (Date Abstract Data Type) representation into the external binary protocol format for transmission to clients. This function is the counterpart to `date_recv` and is part of PostgreSQL's type input/output system. It uses the PostgreSQL binary protocol functions to serialize the date value as a 32-bit integer into a bytea result that can be transmitted to clients using the binary protocol.

## Parameters / Member Variables
- `date`: DateADT value (extracted from function arguments) representing the internal date to be converted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT: Macro to extract DateADT argument from function call
  - [pq_begintypsend](../p/pq_begintypsend.md): Initializes a StringInfo buffer for binary output
  - [pq_sendint32](../p/pq_sendint32.md): Writes a 32-bit integer to the output buffer
  - [pq_endtypsend](../p/pq_endtypsend.md): Finalizes the output buffer and returns bytea
  - PG_RETURN_BYTEA_P: Macro to return bytea values from PostgreSQL functions
- Called from (representative examples):
  - No direct references found (likely referenced through function pointers in type system)

## Notes and Other Information
- This function is part of PostgreSQL's binary I/O system for the date data type
- The date is serialized as a 32-bit integer in the binary format
- No validation is performed since the input is assumed to be a valid DateADT value
- The function follows PostgreSQL's standard function calling conventions using PG_FUNCTION_ARGS
- Used by the PostgreSQL binary protocol when clients request date values in binary format rather than text format