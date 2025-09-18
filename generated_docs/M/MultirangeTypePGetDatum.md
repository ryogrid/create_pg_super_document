# MultirangeTypePGetDatum

## Location
src/include/utils/multirangetypes.h: 60 - 64

## Overview
MultirangeTypePGetDatum is an inline function that converts a MultirangeType pointer to a PostgreSQL Datum value for use in the function manager (fmgr) system.

## Definition
```c
static inline Datum
MultirangeTypePGetDatum(const MultirangeType *X)
```

## Detailed Description
This function serves as a type conversion utility that wraps a MultirangeType pointer into PostgreSQL's universal Datum container. It performs the inverse operation of DatumGetMultirangeTypeP, converting from the specific multirange type back to the generic Datum format that PostgreSQL's function manager system expects.

The function takes a const MultirangeType pointer and uses the standard PointerGetDatum macro to wrap it as a Datum. This conversion is essential when multirange values need to be returned from functions or passed through PostgreSQL's internal function call mechanisms. The const qualifier indicates that this conversion doesn't modify the original multirange data.

Being implemented as a static inline function ensures optimal performance with no function call overhead, which is important since this conversion happens frequently in multirange operations.

## Parameters / Member Variables
- `X`: A const MultirangeType pointer that needs to be converted to a Datum value

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md) (implicit - standard PostgreSQL macro for pointer-to-Datum conversion)
  - MultirangeType (parameter type)
- Called from (representative examples):
  - PG_RETURN_MULTIRANGE_P (macro for returning multirange values from functions)

## Notes and Other Information
- Complements DatumGetMultirangeTypeP by providing the reverse conversion
- The const parameter ensures the function doesn't modify the input data
- Critical for PostgreSQL's function return value mechanism
- Part of the standard pattern for PostgreSQL data type implementations
- Used primarily through the PG_RETURN_MULTIRANGE_P macro in function implementations
- Essential component of the multirange type system's integration with PostgreSQL's type infrastructure