# float4_lt

## Location
[src/include/utils/float.h:286-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L286-L291)

## Overview
Compares two single-precision floating-point numbers to determine if the first is less than the second, handling NaN values according to PostgreSQL semantics.

## Definition
```c
static inline bool float4_lt(const float4 val1, const float4 val2)
```

## Detailed Description
This inline function implements less-than comparison for single-precision floating-point numbers (float4) with proper NaN handling. The function follows PostgreSQL semantics for ordering comparisons where NaN is considered greater than any non-NaN value. This means any comparison where val1 is NaN returns false (NaN is not less than anything), while any non-NaN value is considered less than NaN.

The implementation first checks that val1 is not NaN (since NaN cannot be less than anything). If val1 is not NaN, it returns true if val2 is NaN (non-NaN < NaN) or if the standard comparison val1 < val2 is true.

## Parameters / Member Variables
- `val1`: First single-precision floating-point value (left operand)
- `val2`: Second single-precision floating-point value (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - isnan (standard C library function for NaN detection)
  - float4 (typedef for single-precision floating-point)
- Called from (representative examples):
  - [float4lt](float4lt.md) (SQL-callable less-than function)
  - [float4smaller](float4smaller.md) (minimum value function)
  - [float4_cmp_internal](float4_cmp_internal.md) (comparison utility function)
  - [float4_min](float4_min.md) (inline minimum function)

## Notes and Other Information
- This is an inline function defined in utils/float.h for performance
- Implements PostgreSQL semantics where NaN > any non-NaN value in ordering
- Used in SQL comparison operations and aggregate functions like MIN/MAX
- Part of the consistent family of float4 comparison functions
- The function is located at src/include/utils/float.h:286-291

## Simplified Source

```c
static inline bool
float4_lt(const float4 val1, const float4 val2)
{
    // NaN-aware less-than: non-NaN < NaN is true, NaN < anything is false
    return !isnan(val1) && (isnan(val2) || val1 < val2);
}
```