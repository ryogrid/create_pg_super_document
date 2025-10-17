# regconfigsend

## Location
[src/backend/utils/adt/regproc.c:1415-1430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1415-L1430)

## Overview
Converts a regconfig (text search configuration OID) value to external binary format for transmission or storage.

## Definition

```c
Datum
regconfigsend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function handles the binary output conversion for the regconfig data type, which represents text search configuration OIDs. This function is part of PostgreSQL's type input/output system and is used when regconfig values need to be sent in binary format to external destinations (such as binary protocol communications, binary file formats, or replication streams).

The implementation simply delegates to  since regconfig is internally represented as an OID, making the binary representation identical. This delegation approach is efficient and maintains consistency across related types that share the same underlying representation.

## Parameters / Member Variables
- **Input**:  - Function call information containing the regconfig value to convert
- **Return**:  containing the binary representation of the regconfig value

## Dependencies
- Functions called/Symbols referenced:
  -  - OID binary send function (handles the actual binary conversion)
- Called from:
  - PostgreSQL type system (indirectly via binary protocol handlers)

## Notes and Other Information
- This function is the binary output counterpart to 
- Part of PostgreSQL's binary I/O system for efficient data transfer and storage
- Delegates to  since regconfig and OID have identical binary representations
- Used primarily in binary protocol communications, replication, and binary file operations
- The function signature follows PostgreSQL's standard for type send functions
- Critical for performance in binary data transfer scenarios

## Simplified Source

```c
Datum
regconfigsend(PG_FUNCTION_ARGS)
{
    // Delegate to oidsend since regconfig is internally an OID
    return oidsend(fcinfo);
}
```