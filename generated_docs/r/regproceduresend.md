# regproceduresend

## Location
src/backend/utils/adt/regproc.c: 462 - 477

## Overview
Converts regprocedure values to PostgreSQL's external binary format, serving as the binary output function for the regprocedure data type.

## Definition
```c
Datum regproceduresend(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the binary send function for PostgreSQL's regprocedure data type. It is responsible for converting regprocedure values from their internal representation to PostgreSQL's external binary format for transmission over the wire during client-server communication. Since regprocedure is essentially an OID with special formatting behavior, this function simply delegates to oidsend() which handles the actual OID-to-binary conversion.

The function is part of PostgreSQL's type system infrastructure and is automatically invoked when regprocedure data needs to be sent in binary format, typically during client-server communication using the binary protocol.

## Parameters / Member Variables
- Function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- Input: regprocedure value (accessed via fcinfo)
- Returns: Binary representation in PostgreSQL's external format

## Dependencies
- Functions called/Symbols referenced:
  - oidsend
  - fcinfo (function call context)
- Called from:
  - Automatically invoked by PostgreSQL's type system when sending regprocedure values in binary format

## Notes and Other Information
- This is a standard PostgreSQL type send function following the naming convention of [typename]send
- Implementation is deliberately simple - it just delegates to oidsend since regprocedure and oid have the same binary representation
- Part of the binary I/O infrastructure that enables efficient client-server communication
- The binary format is more compact and faster to process than text format, making it important for performance in high-throughput scenarios
- Used automatically by PostgreSQL when clients use the binary protocol for result retrieval
- Works in conjunction with regprocedurerecv to provide complete binary I/O support for the regprocedure type