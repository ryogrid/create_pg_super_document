# cidout

## Location
[src/backend/utils/adt/xid.c:335-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L335-L347)

## Overview
A PostgreSQL internal function that converts a CommandId (cid) value to its external string representation for output purposes.

## Definition

```c
Datum
cidout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is part of PostgreSQL's type system infrastructure, specifically handling the output conversion of CommandId values. CommandId is an internal PostgreSQL type used to track command sequences within transactions. This function takes a CommandId value and converts it to a human-readable string format using standard C library functions. The function allocates memory for a 16-character result buffer and formats the CommandId as an unsigned long integer.

## Parameters / Member Variables
- Input: CommandId value retrieved via  from the function arguments
- Output: Returns a null-terminated C string via 

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_COMMANDID (macro for extracting CommandId from function args)
  - CommandId (PostgreSQL internal type)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - snprintf (C standard library function)
  - PG_RETURN_CSTRING (macro for returning C string)

- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/xid.c:335-347
- Part of the CommandId type's input/output function suite
- Uses a fixed 16-character buffer for the string representation
- The function follows PostgreSQL's standard pattern for type output functions using the PG_FUNCTION_ARGS interface