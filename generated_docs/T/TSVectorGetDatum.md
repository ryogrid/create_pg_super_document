# TSVectorGetDatum

## Location
[src/include/tsearch/ts_type.h:130-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_type.h#L130-L134)

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
  - [PointerGetDatum](../P/PointerGetDatum.md) (macro for converting pointers to Datum values)
  - [TSVectorData](TSVectorData.md) (type parameter)
- Called from (representative examples):
  - [compute_tsvector_stats](../c/compute_tsvector_stats.md)
  - [ts_match_tt](../t/ts_match_tt.md)
  - [ts_match_tq](../t/ts_match_tq.md)
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md)
  - PG_RETURN_TSVECTOR

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Takes a const pointer parameter, indicating the TSVector data should not be modified during conversion
- Essential for returning TSVector values from PostgreSQL functions
- Part of the fmgr (function manager) interface functions for text search operations
- Used in conjunction with PG_RETURN_TSVECTOR macro for function return values

## Simplified Source

```c
static inline Datum TSVectorGetDatum(const TSVectorData *X) {
    // Convert TSVector pointer to Datum for function interface
    return PointerGetDatum(X);
}
```