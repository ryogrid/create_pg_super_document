# Float4GetDatum

## Location
[src/include/postgres.h:475-493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L475-L493)

## Overview
Converts a 4-byte floating point value to a PostgreSQL Datum representation, using union manipulation to preserve the exact bit pattern of the float value.

## Definition
```c
static inline Datum Float4GetDatum(float4 X)
```

## Detailed Description
Float4GetDatum converts a 4-byte floating point value (float4) into PostgreSQL's universal Datum type. The implementation uses a union to safely reinterpret the bit pattern of the float4 value as an int32 value, which is then converted to a Datum using Int32GetDatum().

This approach is necessary because many machine architectures handle integer and floating-point function parameters/results differently. The union ensures that the exact bit representation of the floating-point value is preserved when converting to the Datum format, allowing for accurate reconstruction later using DatumGetFloat4().

The function is implemented as an inline function rather than a macro to properly handle these machine-specific differences in parameter passing conventions.

## Parameters / Member Variables
- `X`: The 4-byte floating point value to be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [Int32GetDatum](../I/Int32GetDatum.md) (converts the int32 representation to a Datum)
  - float4 (PostgreSQL's 4-byte floating point type)
- Called from (representative examples):
  - [index_store_float8_orderby_distances](../i/index_store_float8_orderby_distances.md)
  - [InsertPgClassTuple](../I/InsertPgClassTuple.md)
  - [EnumValuesCreate](../E/EnumValuesCreate.md)
  - [AddEnumLabel](../A/AddEnumLabel.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [update_attstats](../u/update_attstats.md)
  - [serialize_expr_stats](../s/serialize_expr_stats.md)
  - PG_RETURN_FLOAT4

## Notes and Other Information
- Implemented as an inline function rather than a macro to handle machine-specific differences in parameter passing
- Uses union type punning to safely convert between float4 and int32 representations
- Counterpart to DatumGetFloat4() for the reverse conversion
- Essential for storing float4 values in PostgreSQL's internal data structures
- Part of PostgreSQL's type conversion system for floating-point values
- Located in src/include/postgres.h:475-493

## Simplified Source

```c
static inline Datum Float4GetDatum(float4 X) {
    // Use union to safely convert float4 bits to int32
    union {
        float4 value;
        int32  retval;
    } myunion;

    // Store float value and extract as int32
    myunion.value = X;

    // Convert int32 representation to Datum
    return Int32GetDatum(myunion.retval);
}
```