# DatumGetFloat4

## Location
src/include/postgres.h: 458 - 474

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
  - DatumGetInt32 (extracts the int32 representation from the Datum)
  - float4 (PostgreSQL's 4-byte floating point type)
- Called from (representative examples):
  - bernoulli_samplescangetsamplesize
  - bernoulli_beginsamplescan
  - system_samplescangetsamplesize
  - system_beginsamplescan
  - btfloat4fastcmp
  - convert_numeric_to_scalar
  - PG_GETARG_FLOAT4
  - PLyFloat_FromFloat4

## Notes and Other Information
- Implemented as an inline function rather than a macro to handle machine-specific differences in parameter passing
- Uses union type punning to safely convert between int32 and float4 representations
- Part of PostgreSQL's type conversion system for floating-point values
- Essential for extracting float4 values from function arguments and stored data
- Located in src/include/postgres.h:458-474