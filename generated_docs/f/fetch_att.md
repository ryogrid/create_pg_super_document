# fetch_att

## Location
src/include/access/tupmacs.h: 52 - 85

## Overview
Extracts attribute data from tuple storage and converts it to a Datum based on the attribute's byval and length properties.

## Definition
static inline Datum fetch_att(const void *T, bool attbyval, int attlen)

## Detailed Description
The fetch_att function is a fundamental tuple manipulation utility that extracts attribute data from raw tuple storage and converts it to PostgreSQL's internal Datum representation. It handles both by-value and by-reference attributes. For by-value attributes, it performs appropriate type casting based on the attribute length (1, 2, 4, or 8 bytes) and uses the corresponding DatumGet functions. For by-reference attributes, it simply returns a pointer to the data location.

## Parameters / Member Variables
- `T`: Pointer to the attribute data location in tuple storage
- `attbyval`: Boolean indicating if the attribute is stored by value (true) or by reference (false)  
- `attlen`: Length of the attribute in bytes for by-value types

## Dependencies
- Functions called/Symbols referenced:
  - CharGetDatum (for 1-byte values)
  - Int16GetDatum (for 2-byte values)
  - Int32GetDatum (for 4-byte values)
  - PointerGetDatum (for by-reference values)
  - SIZEOF_DATUM (compile-time constant)
- Called from (representative examples):
  - brin_range_deserialize
  - ExecEvalScalarArrayOp
  - deconstruct_array
  - array_iterate
  - range_deserialize
  - fetchatt (wrapper function)

## Notes and Other Information
- This is an inline function optimized for frequent use in tuple processing operations
- The function handles platform-specific differences through SIZEOF_DATUM conditional compilation
- For unsupported byval lengths, it throws an ERROR to prevent data corruption
- This function is the complement to store_att_byval for reading attribute data
- Critical for array processing, range types, statistics, and general tuple deformation operations