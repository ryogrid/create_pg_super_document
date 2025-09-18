# DatumGetInt32

## Location
src/include/postgres.h: 202 - 211

## Overview
Extracts a 32-bit signed integer value from PostgreSQL's internal Datum representation, providing type-safe conversion from Datum to int32.

## Definition


## Detailed Description
DatumGetInt32 is a static inline function that converts a PostgreSQL Datum value back to a 32-bit signed integer (int32). This function is the counterpart to Int32GetDatum, providing the reverse conversion from PostgreSQL's universal Datum type to a native C integer type. The function performs a simple cast operation, which is safe because 32-bit integers are stored directly within the Datum value without additional encoding or indirection.

## Parameters / Member Variables
- : The Datum value to be converted to a 32-bit signed integer

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple cast operation)
- Called from (representative examples):
  - printsimple (src/backend/access/common/printsimple.c:104)
  - collectMatchBitmap (src/backend/access/gin/ginget.c:193)
  - ExecInterpExpr (src/backend/executor/execExprInterp.c:1417, 1427)
  - array_cmp (src/backend/utils/adt/arrayfuncs.c:4070)
  - PG_GETARG_INT32 macro (src/include/fmgr.h:269)
  - ApplyInt32SortComparator (src/include/utils/sortsupport.h:326-327)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it widely available throughout PostgreSQL
- Extensively used throughout PostgreSQL for extracting integer values from Datum parameters in functions
- Critical for type system operations, sorting, comparison functions, and data processing
- Often used in conjunction with PG_GETARG_INT32 macro for extracting function arguments
- The function assumes the Datum contains a valid 32-bit signed integer value - no validation is performed
- Performance-critical as it's used in hot code paths like sorting and comparison operations