# DatumGetFloat8

## Location
[src/include/postgres.h:494-518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L494-L518)

## Overview
Extracts an 8-byte floating point value from a PostgreSQL Datum, handling platform-specific differences in how 64-bit floating point values are stored and passed.

## Definition
```c
static inline float8 DatumGetFloat8(Datum X)
```

## Detailed Description
DatumGetFloat8 converts a PostgreSQL Datum back to an 8-byte floating point value (float8). The implementation varies based on the `USE_FLOAT8_BYVAL` compilation flag, which determines how 64-bit floating point values are handled on the target platform:

- When `USE_FLOAT8_BYVAL` is defined (typically on 64-bit platforms), the function uses a union to reinterpret the int64 representation as a float8 value, similar to the float4 conversion but for 64-bit values.
- When `USE_FLOAT8_BYVAL` is not defined (typically on 32-bit platforms), the function treats the Datum as a pointer to a float8 value stored in allocated memory and dereferences it.

This conditional compilation abstracts away the platform-specific details of float8 storage, providing a consistent interface regardless of whether float8 values are passed by value or by reference.

## Parameters / Member Variables
- `X`: The Datum containing the float8 value to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt64](DatumGetInt64.md) (conditionally, when USE_FLOAT8_BYVAL is defined)
  - [DatumGetPointer](DatumGetPointer.md) (conditionally, when USE_FLOAT8_BYVAL is not defined)
  - USE_FLOAT8_BYVAL (compilation flag determining float8 passing convention)
- Called from (representative examples):
  - [build_distances](../b/build_distances.md)
  - [gistindex_keytest](../g/gistindex_keytest.md)
  - point_point_distance
  - [spg_kd_choose](../s/spg_kd_choose.md)
  - [spg_kd_inner_consistent](../s/spg_kd_inner_consistent.md)
  - [restriction_selectivity](../r/restriction_selectivity.md)
  - [join_selectivity](../j/join_selectivity.md)
  - [width_bucket_array_float8](../w/width_bucket_array_float8.md)
  - [btfloat8fastcmp](../b/btfloat8fastcmp.md)
  - [float8_lerp](../f/float8_lerp.md)
  - [scalararraysel](../s/scalararraysel.md)
  - [convert_numeric_to_scalar](../c/convert_numeric_to_scalar.md)
  - PG_GETARG_FLOAT8

## Notes and Other Information
- The function hides whether float8 is passed by value or by reference, providing a uniform interface
- Implemented as an inline function for performance optimization
- Uses union type punning on 64-bit platforms to safely convert between int64 and float8 representations
- On 32-bit platforms, accesses float8 values through pointer dereferencing
- Essential for extracting float8 values from function arguments and stored data across different architectures
- Part of PostgreSQL's type conversion system that ensures consistent handling of 64-bit floating point values
- Located in src/include/postgres.h:494-518

## Simplified Source

```c
static inline float8
DatumGetFloat8(Datum X)
{
#ifdef USE_FLOAT8_BYVAL
    // On 64-bit platforms: reinterpret int64 as float8
    union {
        int64   value;
        float8  retval;
    } myunion;

    myunion.value = DatumGetInt64(X);
    return myunion.retval;
#else
    // On 32-bit platforms: dereference pointer to float8
    return *((float8 *) DatumGetPointer(X));
#endif
}
```