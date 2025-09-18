# DatumGetPathPCopy

## Location
src/include/utils/geo_decls.h: 207 - 211

## Overview
DatumGetPathPCopy is an inline function that converts a PostgreSQL Datum value to a PATH pointer, creating a copy during TOAST decompression to ensure the caller owns the memory.

## Definition
```c
static inline PATH *
DatumGetPathPCopy(Datum X)
```

## Detailed Description
This function serves as a type-safe wrapper for converting PostgreSQL Datum values to PATH geometry pointers with guaranteed memory ownership. Unlike DatumGetPathP, this function uses PG_DETOAST_DATUM_COPY which ensures that a copy of the data is made, giving the caller ownership of the memory. This is important when the PATH object needs to be modified or when it needs to persist beyond the current memory context.

## Parameters / Member Variables
- `X`: The input Datum value that should contain a PATH geometry object

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY (macro for TOAST decompression with copying)
  - PATH (geometric data type)
- Called from (representative examples):
  - PG_GETARG_PATH_P_COPY (macro for function argument extraction with copy)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Part of PostgreSQL's geometric data type conversion utilities
- Always creates a copy of the data, ensuring caller owns the memory
- Used when the PATH object will be modified or needs to persist beyond current context
- More expensive than DatumGetPathP due to copying, but provides memory safety