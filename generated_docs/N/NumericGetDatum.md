# NumericGetDatum

## Location
src/include/utils/numeric.h: 73 - 77

## Overview
NumericGetDatum is an inline function that converts a Numeric pointer to a Datum value for use in PostgreSQL's function manager interface and data type system.

## Definition
```c
static inline Datum
NumericGetDatum(Numeric X)
```

## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) interface to convert a Numeric pointer into a Datum value. It simply wraps the PointerGetDatum macro to cast the Numeric pointer to a Datum type. This conversion is essential for returning numeric values from PostgreSQL functions, storing them in tuples, passing them between functions, and integrating with PostgreSQL's type system. The function is the reverse operation of DatumGetNumeric, enabling bidirectional conversion between the internal Numeric representation and the generic Datum interface used throughout PostgreSQL.

## Parameters / Member Variables
- `X`: A Numeric pointer that needs to be converted to a Datum value

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum (implicitly called, converts pointer to Datum)
  - Numeric (data type)
- Called from (representative examples):
  - ExecGetJsonValueItemString
  - cash_numeric
  - numeric_cash
  - numeric_to_cstring
  - numeric_is_less
  - numeric_absolute
  - numeric_half_rounded
  - numeric_truncated_divide
  - pg_size_bytes
  - numeric_to_number
  - numeric_to_char
  - jsonb_numeric
  - jsonb_int2/int4/int8/float4/float8
  - JsonbHashScalarValue
  - executeItemOptUnwrapTarget
  - executeUnaryArithmExpr
  - executeNumericItemMethod
  - compareNumeric
  - generate_series_step_numeric
  - numeric_float8/float4
  - numeric_poly_avg
  - numeric_avg
  - int8_sum/avg
  - pg_lsn_pli/mii
  - timestamp_part_common
  - PG_RETURN_NUMERIC

## Notes and Other Information
- This is an inline function defined in src/include/utils/numeric.h for performance optimization
- Part of the fmgr interface macros used throughout PostgreSQL for type conversions
- Essential for any function that needs to return Numeric values as Datum
- Used extensively in numeric operations, JSON processing, formatting functions, and aggregate functions
- The conversion is essentially a type cast since both Numeric and Datum are pointer-sized values
- Critical component for PostgreSQL's polymorphic function system and data type abstraction