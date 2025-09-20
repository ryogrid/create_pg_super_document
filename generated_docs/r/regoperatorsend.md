# regoperatorsend

## Location
[src/backend/utils/adt/regproc.c:866-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L866-L881)

## Overview
Converts regoperator data type to binary format for transmission or storage, used for serializing regoperator values.

## Definition

```c
Datum
regoperatorsend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a send function for the  data type in PostgreSQL. It handles the conversion of internal  values to binary format suitable for network transmission or binary storage. Since  is essentially an OID (Object Identifier), this function simply delegates to the existing  function, sharing the same binary format and conversion logic.

This function is part of PostgreSQL's type system infrastructure, specifically handling binary input/output operations for the  type. It's typically used in contexts where data needs to be serialized, such as network communication protocols or binary storage formats.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: The regoperator value to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  -  - The actual implementation function for sending OID values in binary format
  -  - Function call information structure passed to the delegated function

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function demonstrates code reuse in PostgreSQL's type system - since  is fundamentally an OID, it reuses the existing OID binary serialization logic
- Part of the binary I/O function quartet (input, output, receive, send) required for each PostgreSQL data type
- The comment "Exactly the same as oidsend, so share code" explicitly documents the design decision to reuse existing functionality
- This function would typically be registered in the PostgreSQL type system as the send function for the  type
- Works in conjunction with  to provide complete binary serialization/deserialization capability