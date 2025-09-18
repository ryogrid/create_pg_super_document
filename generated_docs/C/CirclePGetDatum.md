# CirclePGetDatum

## Location
src/include/utils/geo_decls.h: 271 - 274

## Overview
Converts a CIRCLE pointer to a Datum value, enabling storage and manipulation of geometric circle data within PostgreSQLs internal data handling system.

## Definition
```c
static inline Datum
CirclePGetDatum(const CIRCLE *X)
```

## Detailed Description
CirclePGetDatum is an inline utility function that converts a CIRCLE pointer into a Datum value. It serves as the counterpart to DatumGetCircleP, providing the opposite conversion direction in PostgreSQLs geometric data type handling infrastructure. This function wraps the generic PointerGetDatum function with type-specific semantics for CIRCLE data structures.

The function is essential for returning CIRCLE values from PostgreSQL functions and storing them in the database systems internal format. It takes a const CIRCLE pointer, indicating that the source data will not be modified during the conversion process.

## Parameters / Member Variables
- `X`: A const pointer to the CIRCLE structure to be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum
  - CIRCLE (struct type)
- Called from (representative examples):
  - gist_point_consistent
  - PG_RETURN_CIRCLE_P (macro)

## Notes and Other Information
- This is a static inline function defined in src/include/utils/geo_decls.h:271-274
- Part of PostgreSQLs geometric data type conversion infrastructure  
- The const qualifier on the parameter indicates the CIRCLE data is read-only during conversion
- Used primarily in function return operations and GiST indexing contexts
- The CIRCLE struct being converted contains a center point (Point) and radius (float8)
- Essential for the fmgr (function manager) interface when returning CIRCLE values from C functions