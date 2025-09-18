# TSVectorGetDatum

## Location
src/include/tsearch/ts_type.h: 130 - 134

## Overview
Converts a TSVector pointer to a PostgreSQL Datum value for use in the function manager interface and SQL operations.

## Definition
```c
static inline Datum TSVectorGetDatum(const TSVectorData *X)
```

## Detailed Description
TSVectorGetDatum is an inline utility function that converts a TSVector pointer to a PostgreSQL Datum. This is the inverse operation of DatumGetTSVector, allowing TSVector data structures to be returned from PostgreSQL functions or passed as arguments to other functions within the PostgreSQL function manager (fmgr) interface.

The function uses PointerGetDatum to perform the conversion, treating the TSVector as a pointer-based datum. This is appropriate since TSVector is a variable-length data type that is always passed by reference in PostgreSQL.

## Parameters / Member Variables
- `X`: Pointer to a constant TSVectorData structure to be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum (macro for converting pointers to Datum values)
  - TSVectorData (type parameter)
- Called from (representative examples):
  - compute_tsvector_stats
  - ts_match_tt
  - ts_match_tq
  - tsvector_update_trigger
  - PG_RETURN_TSVECTOR

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Takes a const pointer parameter, indicating the TSVector data should not be modified during conversion
- Essential for returning TSVector values from PostgreSQL functions
- Part of the fmgr (function manager) interface functions for text search operations
- Used in conjunction with PG_RETURN_TSVECTOR macro for function return values