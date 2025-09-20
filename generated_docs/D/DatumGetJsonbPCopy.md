# DatumGetJsonbPCopy

## Location
[src/include/utils/jsonb.h:380-385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/jsonb.h#L380-L385)

## Overview
DatumGetJsonbPCopy is a convenience macro function that converts a Datum value to a Jsonb pointer, creating a writable copy through TOAST decompression.

## Definition

```c
static inline Jsonb *
DatumGetJsonbPCopy(Datum d)
```
## Detailed Description
This inline function provides a way to extract a writable Jsonb pointer from a Datum value by creating a copy. Unlike DatumGetJsonbP which may return a read-only reference to TOASTed data, DatumGetJsonbPCopy uses PG_DETOAST_DATUM_COPY to ensure that a modifiable copy is returned. This is essential when the calling code needs to modify the Jsonb structure, as direct modification of TOASTed data could lead to corruption or unexpected behavior. The function ensures memory safety by providing a dedicated copy that can be safely modified.

## Parameters / Member Variables
- : The input Datum value that contains a JSONB value, potentially in TOASTed form

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY
  - Jsonb (type)
- Called from (representative examples):
  - PG_GETARG_JSONB_P_COPY

## Notes and Other Information
- This is a static inline function defined in src/include/utils/jsonb.h
- Creates a writable copy of the JSONB data, unlike DatumGetJsonbP which may return read-only references
- Essential for operations that need to modify JSONB structures in-place
- Used less frequently than DatumGetJsonbP due to the performance overhead of copying
- The copy operation ensures memory safety when modifying JSONB data
- Part of the convenience macro family for JSONB type conversion