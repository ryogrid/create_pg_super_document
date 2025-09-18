# NumericGetDatum

## Location
[src/include/utils/numeric.h:73-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/numeric.h#L73-L77)

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
  - [PointerGetDatum](../P/PointerGetDatum.md) (implicitly called, converts pointer to Datum)
  - Numeric (data type)
- Called from (representative examples):
  - [ExecGetJsonValueItemString](../E/ExecGetJsonValueItemString.md)
  - [cash_numeric](../c/cash_numeric.md)
  - [numeric_cash](../n/numeric_cash.md)
  - [numeric_to_cstring](../n/numeric_to_cstring.md)
  - [numeric_is_less](../n/numeric_is_less.md)
  - [numeric_absolute](../n/numeric_absolute.md)
  - [numeric_half_rounded](../n/numeric_half_rounded.md)
  - [numeric_truncated_divide](../n/numeric_truncated_divide.md)
  - [pg_size_bytes](../p/pg_size_bytes.md)
  - [numeric_to_number](../n/numeric_to_number.md)
  - [numeric_to_char](../n/numeric_to_char.md)
  - [jsonb_numeric](../j/jsonb_numeric.md)
  - [jsonb_int2](../j/jsonb_int2.md)/int4/int8/float4/float8
  - [JsonbHashScalarValue](../J/JsonbHashScalarValue.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeUnaryArithmExpr](../e/executeUnaryArithmExpr.md)
  - [executeNumericItemMethod](../e/executeNumericItemMethod.md)
  - [compareNumeric](../c/compareNumeric.md)
  - generate_series_step_numeric
  - [numeric_float8](../n/numeric_float8.md)/float4
  - [numeric_poly_avg](../n/numeric_poly_avg.md)
  - [numeric_avg](../n/numeric_avg.md)
  - [int8_sum](../i/int8_sum.md)/avg
  - [pg_lsn_pli](../p/pg_lsn_pli.md)/mii
  - [timestamp_part_common](../t/timestamp_part_common.md)
  - PG_RETURN_NUMERIC

## Notes and Other Information
- This is an inline function defined in src/include/utils/numeric.h for performance optimization
- Part of the fmgr interface macros used throughout PostgreSQL for type conversions
- Essential for any function that needs to return Numeric values as Datum
- Used extensively in numeric operations, JSON processing, formatting functions, and aggregate functions
- The conversion is essentially a type cast since both Numeric and Datum are pointer-sized values
- Critical component for PostgreSQL's polymorphic function system and data type abstraction