# regopersend

## Location
[src/backend/utils/adt/regproc.c:623-638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L623-L638)

## Overview
Converts regoper data type to external binary format for network transmission and binary storage.

## Definition
```c
Datum regopersend(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regopersend` function is a binary output function for the regoper data type in PostgreSQL. It handles the conversion of regoper values from their internal representation to PostgreSQL's external binary wire format. Since regoper is internally represented as an OID (Object Identifier), this function simply delegates to `oidsend` to perform the actual binary serialization.

This function is part of PostgreSQL's type input/output framework and is automatically called by the system when regoper data needs to be transmitted in binary format, such as during network communication with clients using the binary protocol or when writing to binary storage formats.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments

## Dependencies
- Functions called/Symbols referenced:
  - [oidsend](../o/oidsend.md): Binary send function for OID type that performs the actual binary serialization

- Called from (representative examples):
  - No direct references found (typically called via PostgreSQL's type system during binary I/O)

## Notes and Other Information
- This is a binary output function, complementing the text output function `regoperout`
- The function shares implementation with `oidsend` since regoper has the same binary representation as OID
- Part of PostgreSQL's binary I/O protocol for efficient data transfer
- Used primarily in client-server communication when binary protocol is enabled
- The binary format is more compact and faster to process than text format
- The simplicity reflects that regoper and OID have identical binary representations

## Simplified Source

```c
Datum regopersend(PG_FUNCTION_ARGS) {
    // regoper has same binary format as OID, so delegate to oidsend
    return oidsend(fcinfo);
}
```