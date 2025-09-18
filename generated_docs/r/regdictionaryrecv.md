# regdictionaryrecv

## Location
src/backend/utils/adt/regproc.c: 1516 - 1525

## Overview
Converts external binary format to regdictionary type, handling binary input/output operations for dictionary registry OIDs.

## Definition
```c
Datum regdictionaryrecv(PG_FUNCTION_ARGS)
```

## Detailed Description
The regdictionaryrecv function is responsible for converting external binary format data to PostgreSQL's regdictionary type. This function is part of the registry type system that provides symbolic names for various database objects. The regdictionary type specifically handles references to text search dictionaries. 

The implementation is straightforward - it delegates all processing to the oidrecv function, as the underlying representation of regdictionary is identical to that of a standard OID. This design follows PostgreSQL's pattern of reusing OID handling logic across different registry types while maintaining type safety at the SQL level.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
  - Input binary data to be converted
  - Function context and metadata
  - Return value storage

## Dependencies
- Functions called/Symbols referenced:
  - [oidrecv](../o/oidrecv.md) (delegates all processing to this function)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function registry)

## Notes and Other Information
- Located in src/backend/utils/adt/regproc.c:1516-1525
- Part of the regdictionary type input/output function suite
- Shares implementation with oidrecv due to identical binary representation
- Used internally by PostgreSQL's type system for binary I/O operations
- The regdictionary type allows referencing text search dictionaries by name while storing them as OIDs internally