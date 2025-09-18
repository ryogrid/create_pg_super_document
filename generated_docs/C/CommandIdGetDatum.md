# CommandIdGetDatum

## Location
[src/include/postgres.h:302-311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L302-L311)

## Overview
Converts a CommandId value to a Datum type, providing the inverse operation of DatumGetCommandId for storing command identifiers in PostgreSQL's generic Datum representation.

## Definition
```c
static inline Datum CommandIdGetDatum(CommandId X)
```

## Detailed Description
CommandIdGetDatum is a static inline function that performs a simple type cast from CommandId to Datum. This function is the complementary operation to DatumGetCommandId, allowing CommandId values to be stored in PostgreSQL's generic Datum container. This conversion is essential for the PostgreSQL type system, enabling command identifiers to be passed through the generic function parameter and return value mechanisms that use Datum as the universal data type.

## Parameters / Member Variables
- `X`: The input CommandId value to be converted to Datum representation

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type)
  - Datum (type)
- Called from (representative examples):
  - [heap_getsysattr](../h/heap_getsysattr.md)
  - PG_RETURN_COMMANDID

## Notes and Other Information
- This is a static inline function defined in postgres.h, making it available throughout the PostgreSQL codebase
- The function performs a simple cast with no validation, directly converting the CommandId to Datum
- Used in system attribute access functions and return macros for CommandId values
- Part of PostgreSQL's broader Datum conversion API that provides type-safe conversion methods between specific types and the generic Datum representation