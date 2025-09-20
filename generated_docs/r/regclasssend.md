# regclasssend

## Location
[src/backend/utils/adt/regproc.c:1010-1025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1010-L1025)

## Overview
Converts regclass type values to external binary format by delegating to the standard OID binary output function.

## Definition

```c
Datum
regclasssend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the binary output function for the  data type. It handles the conversion of internal regclass values to binary format data suitable for network transmission or binary storage.

Since regclass is internally represented as an OID (Object Identifier), this function simply delegates to  to perform the actual OID-to-binary conversion. This approach leverages the fact that regclass and OID have identical binary representations - the semantic difference between the types is handled at the text I/O level, not in binary format.

This function is part of PostgreSQL's type input/output function framework, specifically handling binary format output for regclass values in contexts such as:
- Network protocol communication using binary format
- Binary data export and storage
- Inter-process communication with binary protocols
- Client-server data exchange in binary mode

## Parameters / Member Variables
- Input:  (FunctionCallInfo) - Function call information containing regclass value to convert

## Dependencies
- Functions called/Symbols referenced:
  -  - Binary output function for OID type

- Called from (representative examples):
  - (No direct references found - typically called by PostgreSQL's type system infrastructure)

## Notes and Other Information
- Shares implementation with oidsend due to identical binary representation between regclass and OID types
- Part of the complete regclass type I/O function set (regclassin, regclassout, regclassrecv, regclasssend)
- Used in binary protocol contexts rather than text-based input/output operations
- The binary format for regclass output is a 4-byte big-endian integer, identical to OID
- Automatically invoked by PostgreSQL's type system when binary output is required
- No additional processing or validation is performed beyond the standard OID binary conversion
- Complements regclassrecv for complete binary format support