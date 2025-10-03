# regcollationrecv

## Location
[src/backend/utils/adt/regproc.c:1144-1153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1144-L1153)

## Overview
Converts external binary format data to regcollation type, serving as the binary input function for the regcollation data type.

## Definition

```c
Datum
regcollationrecv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the binary receive function for the regcollation data type. It handles the conversion of binary data received over the network or from storage into the internal regcollation representation. The function is a thin wrapper that delegates all processing to the  function, since regcollation internally uses the same binary format as the OID type.

This function is part of PostgreSQL's type system infrastructure and is automatically called when:
- Receiving binary-format data through the PostgreSQL protocol
- Reading binary data from disk storage
- Processing binary data in replication streams

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
## Dependencies
- Functions called/Symbols referenced:
  - : Handles the actual binary-to-OID conversion since regcollation uses OID format internally
- Called from:
  - PostgreSQL type system (registered as binary receive function for regcollation type)

## Notes and Other Information
- This is essentially a type-safe wrapper around  since regcollation is implemented as an OID subtype
- The function maintains type system consistency while reusing existing OID binary handling logic
- Part of the standard set of I/O functions required for any PostgreSQL data type
- Works in conjunction with  for binary serialization/deserialization