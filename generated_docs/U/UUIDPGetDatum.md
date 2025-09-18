# UUIDPGetDatum

## Location
src/include/utils/uuid.h: 27 - 31

## Overview
Converts a pg_uuid_t pointer to a PostgreSQL Datum for use in the function manager (fmgr) interface system.

## Definition
```c
static inline Datum UUIDPGetDatum(const pg_uuid_t *X)
```

## Detailed Description
UUIDPGetDatum is an inline function macro that provides a type-safe conversion from a pg_uuid_t pointer to a Datum. This function is part of PostgreSQL's function manager interface system, which standardizes how different data types are passed between functions. It wraps the generic PointerGetDatum() function to provide UUID-specific type safety and semantic clarity.

The function is defined as a static inline function in the header file, making it extremely efficient as it will be inlined by the compiler at call sites, eliminating function call overhead while maintaining type safety.

## Parameters / Member Variables
- `X`: A const pointer to a pg_uuid_t structure containing the UUID data to be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [pg_uuid_t](../p/pg_uuid_t.md)
- Called from (representative examples):
  - PG_RETURN_UUID_P

## Notes and Other Information
- This function is part of the standard PostgreSQL fmgr interface pattern where each data type has corresponding conversion functions
- The const qualifier on the parameter indicates that the UUID data will not be modified during conversion
- Being an inline function, it provides type safety without runtime overhead
- Used primarily in function return paths where UUID values need to be wrapped as Datums