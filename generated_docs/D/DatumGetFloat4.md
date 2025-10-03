# DatumGetFloat4

## Location
[src/include/postgres.h:458-474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L458-L474)

## Overview
Extracts a 4-byte floating point value from a PostgreSQL Datum, handling the type conversion through union manipulation to ensure proper floating-point representation.

## Definition
```c
static inline float4 DatumGetFloat4(Datum X)
```

## Detailed Description
DatumGetFloat4 converts a PostgreSQL Datum back to a 4-byte floating point value (float4). The implementation uses a union to safely reinterpret the bit pattern of an int32 value as a float4 value. This approach is necessary because many machine architectures handle integer and floating-point function parameters/results differently, requiring careful type punning to preserve the exact bit representation.

The function first extracts the int32 representation using DatumGetInt32(), then uses a union to reinterpret those same bits as a float4 value. This ensures that the original floating-point value is correctly reconstructed from its Datum representation.

## Parameters / Member Variables
- `X`: The Datum containing the float4 value to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](DatumGetInt32.md) (extracts the int32 representation from the Datum)
  - float4 (PostgreSQL's 4-byte floating point type)
- Called from (representative examples):
  - [bernoulli_samplescangetsamplesize](../b/bernoulli_samplescangetsamplesize.md)
  - [bernoulli_beginsamplescan](../b/bernoulli_beginsamplescan.md)
  - [system_samplescangetsamplesize](../s/system_samplescangetsamplesize.md)
  - [system_beginsamplescan](../s/system_beginsamplescan.md)
  - [btfloat4fastcmp](../b/btfloat4fastcmp.md)
  - [convert_numeric_to_scalar](../c/convert_numeric_to_scalar.md)
  - PG_GETARG_FLOAT4
  - [PLyFloat_FromFloat4](../P/PLyFloat_FromFloat4.md)

## Notes and Other Information
- Implemented as an inline function rather than a macro to handle machine-specific differences in parameter passing
- Uses union type punning to safely convert between int32 and float4 representations
- Part of PostgreSQL's type conversion system for floating-point values
- Essential for extracting float4 values from function arguments and stored data
- Located in src/include/postgres.h:458-474

## Simplified Source

```c
static inline float4 DatumGetFloat4(Datum X) {
    // Use union to safely reinterpret int32 bits as float4
    union {
        int32   value;
        float4  retval;
    } myunion;

    myunion.value = DatumGetInt32(X);
    return myunion.retval;
}
```