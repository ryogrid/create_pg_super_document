# regcollationsend

## Location
[src/backend/utils/adt/regproc.c:1154-1175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1154-L1175)

## Overview
Converts regcollation type to external binary format, serving as the binary output function for the regcollation data type.

## Definition

```c
Datum
regcollationsend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the binary send function for the regcollation data type. It handles the conversion of internal regcollation representation to binary format for transmission over the network or storage to disk. The function is a thin wrapper that delegates all processing to the  function, since regcollation internally uses the same binary format as the OID type.

This function is part of PostgreSQL's type system infrastructure and is automatically called when:
- Sending binary-format data through the PostgreSQL protocol
- Writing binary data to disk storage
- Processing binary data in replication streams

The binary format produced is compatible with the OID binary format, ensuring efficient storage and transmission.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
## Dependencies
- Functions called/Symbols referenced:
  - : Handles the actual OID-to-binary conversion since regcollation uses OID format internally
- Called from:
  - PostgreSQL type system (registered as binary send function for regcollation type)

## Notes and Other Information
- This is essentially a type-safe wrapper around  since regcollation is implemented as an OID subtype
- The function maintains type system consistency while reusing existing OID binary handling logic
- Part of the standard set of I/O functions required for any PostgreSQL data type
- Works in conjunction with  for binary serialization/deserialization
- The binary format is platform-independent and suitable for network transmission

## Simplified Source

```c
Datum
regcollationsend(PG_FUNCTION_ARGS)
{
    // Delegate to oidsend since regcollation uses identical binary format
    return oidsend(fcinfo);
}
```