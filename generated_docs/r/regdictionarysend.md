# regdictionarysend

## Location
src/backend/utils/adt/regproc.c: 1526 - 1540

## Overview
Converts regdictionary type to external binary format, handling binary output operations for dictionary registry OIDs.

## Definition
```c
Datum regdictionarysend(PG_FUNCTION_ARGS)
```

## Detailed Description
The regdictionarysend function is responsible for converting PostgreSQL's regdictionary type to external binary format data. This function is the counterpart to regdictionaryrecv and is part of the registry type system that provides symbolic names for database objects. The regdictionary type specifically handles references to text search dictionaries.

Similar to its receive counterpart, this function delegates all processing to the oidsend function since the underlying binary representation of regdictionary is identical to that of a standard OID. This design maintains consistency with PostgreSQL's approach of reusing OID handling logic across different registry types while preserving type safety at the SQL level.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
  - regdictionary value to be converted to binary format
  - Function context and metadata
  - Return value storage for binary data

## Dependencies
- Functions called/Symbols referenced:
  - [oidsend](../o/oidsend.md) (delegates all processing to this function)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function registry)

## Notes and Other Information
- Located in src/backend/utils/adt/regproc.c:1526-1540
- Part of the regdictionary type input/output function suite
- Shares implementation with oidsend due to identical binary representation
- Used internally by PostgreSQL's type system for binary I/O operations
- Complements regdictionaryrecv to provide complete binary I/O support for regdictionary type
- The regdictionary type enables referencing text search dictionaries by name while storing them as OIDs internally