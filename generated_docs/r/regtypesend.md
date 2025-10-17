# regtypesend

## Location
[src/backend/utils/adt/regproc.c:1305-1320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1305-L1320)

## Overview
Converts regtype data to external binary format, serving as the binary output function for the regtype data type.

## Definition

```c
Datum
regtypesend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is PostgreSQL's binary send function for the regtype data type. It is responsible for converting an internal regtype value into PostgreSQL's external binary format for transmission or storage. The function is a simple wrapper that delegates all processing to the  function, since regtype is internally represented as an OID.

This function is part of PostgreSQL's type system infrastructure and is called automatically when regtype data needs to be converted to binary format, such as during network communication, binary protocol operations, or when storing data in binary format.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro
  - First argument (index 0): regtype value to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - : OID binary send function that handles the actual binary conversion
- Called from (representative examples):
  - No direct references found in the codebase (called by PostgreSQL's type system infrastructure)

## Notes and Other Information
- Simple delegation to oidsend since regtype is internally an OID
- Part of PostgreSQL's binary I/O system for the regtype data type
- Companion to regtyperecv for binary serialization/deserialization
- Located in src/backend/utils/adt/regproc.c
- Handles binary protocol conversion automatically within PostgreSQL's type system
- Used in client-server communication and binary data storage operations

## Simplified Source

```c
Datum
regtypesend(PG_FUNCTION_ARGS)
{
    // Delegate to oidsend since regtype is internally an OID
    return oidsend(fcinfo);
}
```