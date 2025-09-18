# DatumGetLineP

## Location
src/include/utils/geo_decls.h: 221 - 225

## Overview
DatumGetLineP is an inline function that converts a PostgreSQL Datum value to a LINE pointer for geometric line operations.

## Definition
```c
static inline LINE *
DatumGetLineP(Datum X)
```

## Detailed Description
This function serves as a type-safe wrapper for converting PostgreSQL Datum values to LINE geometry pointers. Unlike PATH objects which may be TOASTed, LINE objects are typically small fixed-size structures that don't require TOAST handling, so this function uses the simpler DatumGetPointer conversion. This is part of PostgreSQL's geometric data type system for representing infinite lines in 2D space.

## Parameters / Member Variables
- `X`: The input Datum value that should contain a LINE geometry object

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetPointer (macro for simple pointer extraction)
  - LINE (geometric data type for infinite lines)
- Called from (representative examples):
  - PG_GETARG_LINE_P (macro for function argument extraction)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Part of PostgreSQL's geometric data type conversion utilities
- Uses DatumGetPointer instead of PG_DETOAST_DATUM since LINE objects are not TOASTed
- LINE represents an infinite line in 2D space, different from PATH which represents a series of connected points
- Simpler than PATH conversion functions due to fixed size of LINE structures