# regconfigrecv

## Location
[src/backend/utils/adt/regproc.c:1405-1414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1405-L1414)

## Overview
Converts external binary format data to a regconfig (text search configuration OID) type.

## Definition

```c
Datum
regconfigrecv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function handles the binary input conversion for the regconfig data type, which represents text search configuration OIDs. This function is part of PostgreSQL's type input/output system and is used when regconfig values are received in binary format from external sources (such as binary protocol communications or binary file formats).

The implementation simply delegates to  since regconfig is internally represented as an OID, making the binary representation identical. This is a common pattern in PostgreSQL where related types share the same internal representation.

## Parameters / Member Variables
- **Input**:  - Function call information containing binary data buffer
- **Return**:  containing the converted regconfig OID value

## Dependencies
- Functions called/Symbols referenced:
  -  - OID binary receive function (handles the actual conversion)
- Called from:
  - PostgreSQL type system (indirectly via binary protocol handlers)

## Notes and Other Information
- This function is the binary input counterpart to  
- Part of PostgreSQL's binary I/O system for efficient data transfer
- Delegates to  since regconfig and OID have identical binary representations
- Used primarily in binary protocol communications and binary file operations
- The function signature follows PostgreSQL's standard for type receive functions