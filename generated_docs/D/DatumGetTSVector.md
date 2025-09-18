# DatumGetTSVector

## Location
src/include/tsearch/ts_type.h: 118 - 123

## Overview
Converts a PostgreSQL Datum value to a TSVector pointer, applying detoasting if necessary for variable-length data types.

## Definition
```c
static inline TSVector DatumGetTSVector(Datum X)
```

## Detailed Description
DatumGetTSVector is an inline utility function that safely converts a PostgreSQL Datum to a TSVector pointer. It uses the PG_DETOAST_DATUM macro to handle potentially compressed or out-of-line stored TSVector data. This is essential for text search operations where TSVector data may be stored in PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) mechanism due to its variable length nature.

The function provides a type-safe conversion from the generic Datum type to the specific TSVector type while ensuring that any compressed or externally stored data is properly detoasted before use.

## Parameters / Member Variables
- `X`: Input Datum value that represents a TSVector to be converted

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (macro for detoasting variable-length data)
  - TSVector (type casting)
- Called from (representative examples):
  - compute_tsvector_stats
  - gtsvector_compress
  - ts_match_tt
  - ts_match_tq
  - ts_accum
  - PG_GETARG_TSVECTOR

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Essential for handling PostgreSQL's TOAST mechanism for variable-length TSVector data
- Used extensively throughout the text search subsystem for type-safe Datum to TSVector conversions
- Part of the fmgr (function manager) interface functions for text search operations