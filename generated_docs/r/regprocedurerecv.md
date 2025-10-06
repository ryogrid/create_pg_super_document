# regprocedurerecv

## Location
[src/backend/utils/adt/regproc.c:452-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L452-L461)

## Overview
Converts external binary format data to regprocedure type, serving as the binary input function for the regprocedure data type.

## Definition
```c
Datum regprocedurerecv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the binary receive function for PostgreSQL's regprocedure data type. It is responsible for converting data from PostgreSQL's external binary format (used in binary protocol communications) into the internal regprocedure representation. Since regprocedure is essentially an OID with special formatting behavior, this function simply delegates to oidrecv() which handles the actual binary-to-OID conversion.

The function is part of PostgreSQL's type system infrastructure and is automatically invoked when regprocedure data is received in binary format, typically during client-server communication using the binary protocol.

## Parameters / Member Variables
- Input: Binary data in PostgreSQL's external format (accessed via fcinfo)
- Returns: regprocedure value (internally represented as an OID)

## Dependencies
- Functions called/Symbols referenced:
  - [oidrecv](../o/oidrecv.md)
  - fcinfo (function call context)
- Called from:
  - Automatically invoked by PostgreSQL's type system when receiving regprocedure values in binary format

## Notes and Other Information
- This is a standard PostgreSQL type receive function following the naming convention of [typename]recv
- Implementation is deliberately simple - it just delegates to oidrecv since regprocedure and oid have the same binary representation
- Part of the binary I/O infrastructure that enables efficient client-server communication
- The binary format is more compact and faster to process than text format, making it important for performance in high-throughput scenarios
- Used automatically by PostgreSQL when clients use the binary protocol for parameter passing or result retrieval

## Simplified Source

```c
Datum regprocedurerecv(PG_FUNCTION_ARGS) {
    // Delegate to oidrecv since regprocedure has same binary format as OID
    return oidrecv(fcinfo);
}
```