# regoperatorrecv

## Location
[src/backend/utils/adt/regproc.c:856-865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L856-L865)

## Overview
Converts external binary format to regoperator data type, used for deserializing regoperator values from binary representation.

## Definition

```c
Datum
regoperatorrecv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a receive function for the  data type in PostgreSQL. It handles the conversion of binary data received over the wire or from storage back into the internal  representation. Since  is essentially an OID (Object Identifier), this function simply delegates to the existing  function, sharing the same binary format and conversion logic.

This function is part of PostgreSQL's type system infrastructure, specifically handling binary input/output operations for the  type. It's typically used in contexts where binary data needs to be deserialized, such as network communication or binary storage formats.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Contains the binary data to be converted to regoperator format

## Dependencies
- Functions called/Symbols referenced:
  -  - The actual implementation function for receiving OID values from binary format
  -  - Function call information structure passed to the delegated function

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function demonstrates code reuse in PostgreSQL's type system - since  is fundamentally an OID, it reuses the existing OID binary deserialization logic
- Part of the binary I/O function quartet (input, output, receive, send) required for each PostgreSQL data type
- The comment "Exactly the same as oidrecv, so share code" explicitly documents the design decision to reuse existing functionality
- This function would typically be registered in the PostgreSQL type system as the receive function for the  type