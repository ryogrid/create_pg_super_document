# regoperrecv

## Location
src/backend/utils/adt/regproc.c: 613 - 622

## Overview
Converts external binary format data to the regoper data type, used for binary input/output operations.

## Definition
```c
Datum regoperrecv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regoperrecv` function is a binary input function for the regoper data type in PostgreSQL. It handles the conversion of data from PostgreSQL's external binary wire format into the internal regoper representation. Since regoper is essentially an OID (Object Identifier) internally, this function simply delegates to `oidrecv` to perform the actual conversion.

This function is part of PostgreSQL's type input/output framework and is typically called automatically by the system when binary data needs to be converted to regoper type, such as during network communication with clients using the binary protocol or when reading from binary storage formats.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Contains binary input buffer and related metadata for conversion

## Dependencies
- Functions called/Symbols referenced:
  - `[oidrecv](../o/oidrecv.md)`: Binary receive function for OID type that performs the actual conversion

- Called from (representative examples):
  - No direct references found (typically called via PostgreSQL's type system during binary I/O)

## Notes and Other Information
- This is a binary input function, complementing the text input function `regoperin`
- The function shares implementation with `oidrecv` since regoper is internally represented as an OID
- Part of PostgreSQL's binary I/O protocol for efficient data transfer
- Used primarily in client-server communication when binary protocol is enabled
- The simplicity of this function reflects that the binary representation of regoper is identical to that of OID