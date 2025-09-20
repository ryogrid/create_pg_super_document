# bit_send

## Location
[src/backend/utils/adt/varbit.c:376-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L376-L390)

## Overview
Converts a bit string to binary format for network transmission or storage, used in PostgreSQL's binary protocol.

## Definition

```c
Datum
bit_send(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that converts a bit string from its internal representation to binary format suitable for network transmission. This function is part of PostgreSQL's type system infrastructure, specifically handling the binary serialization of the  data type. The function simply delegates to  since both fixed-length bit strings () and variable-length bit strings () use the same binary representation format.

The binary format consists of:
1. A 32-bit integer representing the bit length
2. The actual bit data as a sequence of bytes

## Parameters / Member Variables
- : Function call information structure containing the input bit string argument

## Dependencies
- Functions called/Symbols referenced:
  - [varbit_send](../v/varbit_send.md) (delegates all functionality to this function)
- Called from (representative examples):
  - PostgreSQL's type system for binary protocol operations

## Notes and Other Information
- This is a thin wrapper around  since both  and  types share the same binary representation
- Used internally by PostgreSQL when sending bit data in binary format over network connections
- Part of the PostgreSQL function manager (fmgr) system with the standard PG_FUNCTION_ARGS interface
- Located in src/backend/utils/adt/varbit.c:376-390