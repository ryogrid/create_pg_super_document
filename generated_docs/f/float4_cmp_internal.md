# float4_cmp_internal

## Location
[src/backend/utils/adt/float.c:809-818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L809-L818)

## Overview
Internal comparison function that performs three-way comparison between two single-precision floating-point numbers (float4), returning an integer indicating their relative ordering.

## Definition
```c
int float4_cmp_internal(float4 a, float4 b)
```

## Detailed Description
float4_cmp_internal is an internal utility function that implements three-way comparison semantics for single-precision floating-point numbers. It returns a standard comparison result: positive value if the first operand is greater, negative value if the first operand is less, and zero if they are equal. The function leverages float4_gt() and float4_lt() helper functions which handle NaN values according to IEEE 754 standards, where NaN is considered greater than any non-NaN value and comparisons with NaN have specific behavior.

## Parameters / Member Variables
- `a`: First float4 operand for comparison
- `b`: Second float4 operand for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [float4_gt](float4_gt.md): Inline helper function that performs greater-than comparison with NaN handling
  - [float4_lt](float4_lt.md): Inline helper function that performs less-than comparison with NaN handling
- Called from (representative examples):
  - [btfloat4cmp](../b/btfloat4cmp.md): B-tree comparison function for float4 values
  - [btfloat4fastcmp](../b/btfloat4fastcmp.md): Fast B-tree comparison function for float4 values

## Notes and Other Information
- This function implements standard three-way comparison semantics used by sorting and indexing operations
- Returns 1 if a > b, -1 if a < b, and 0 if a == b
- NaN handling follows IEEE 754 standards through the underlying float4_gt() and float4_lt() functions
- Used primarily by B-tree indexing operations for float4 columns
- The function is part of PostgreSQL's comparison operator infrastructure for single-precision floating-point arithmetic
- Located in src/backend/utils/adt/float.c:809-818