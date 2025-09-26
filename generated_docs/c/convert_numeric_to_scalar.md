# convert_numeric_to_scalar

## Location
src/backend/utils/adt/selfuncs.c: 4465 - 4526

## Overview
Converts numeric PostgreSQL data types to double-precision floating-point values for use in selectivity estimation calculations, handling all built-in numeric types including integers, floats, numerics, and OID types.

## Definition


## Detailed Description
This function performs the numeric-specific conversion logic for , converting various PostgreSQL numeric data types to a unified double representation. It handles the following type categories:

1. **Boolean**: Converts boolean values to 0.0 or 1.0
2. **Integers**: Converts int2, int4, int8 to double with appropriate casting
3. **Floating-point**: Directly extracts float4 and float8 values 
4. **Numeric**: Uses  to safely convert NUMERIC to double, clamping out-of-range values to ±HUGE_VAL
5. **OID Types**: Treats all OID-based types (oid, regproc, regclass, etc.) as numeric values by extracting their underlying ObjectId

The function uses PostgreSQL's standard Datum extraction macros to safely convert from the internal Datum representation to C native types before casting to double.

## Parameters
- : The Datum containing the numeric value to convert
- : The OID identifying the specific numeric type
- : Output parameter set to true if the type is unsupported (input value unchanged on success)

## Dependencies
- Functions called:
  - DatumGetBool
  - DatumGetInt16
  - DatumGetInt32
  - DatumGetInt64
  - DatumGetFloat4
  - DatumGetFloat8
  - DatumGetObjectId
  - DirectFunctionCall1
  - numeric_float8_no_overflow
- Called from:
  - convert_to_scalar (in selfuncs.c:4366, 4368, 4370)

## Notes and Other Information
- Returns 0.0 and sets *failure to true for unsupported numeric types
- NUMERIC values that exceed double precision range are safely clamped rather than causing overflow errors
- All registry types (regproc, regclass, regtype, etc.) are treated uniformly as OIDs
- The function assumes the input Datum actually contains a value of the specified type
- Used exclusively within the selectivity estimation subsystem for histogram interpolation and inequality selectivity calculations