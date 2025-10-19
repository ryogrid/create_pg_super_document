# float4_ne

## Location
[src/include/utils/float.h:274-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L274-L279)

## Overview
Compares two single-precision floating-point numbers for inequality, handling NaN values correctly according to PostgreSQL semantics.

## Definition
```c
static inline bool float4_ne(const float4 val1, const float4 val2)
```

## Detailed Description
This inline function implements inequality comparison for single-precision floating-point numbers (float4) with proper NaN handling. The function follows PostgreSQL semantics where NaN values are considered equal to each other but not equal to any other value. This means two NaN values are not considered "not equal" to each other, but a NaN is "not equal" to any non-NaN value.

The implementation uses a conditional expression that first checks if val1 is NaN - if so, it returns whether val2 is NOT NaN (since NaN != non-NaN is true, but NaN != NaN is false). If val1 is not NaN, it returns true if val2 is NaN or if the values are different using standard comparison.

## Parameters / Member Variables
- `val1`: First single-precision floating-point value to compare
- `val2`: Second single-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - isnan (standard C library function for NaN detection)
  - float4 (typedef for single-precision floating-point)
- Called from (representative examples):
  - [float4ne](float4ne.md) (SQL-callable inequality function)

## Notes and Other Information
- This is an inline function defined in utils/float.h for performance
- Handles the PostgreSQL-specific NaN semantics where NaN == NaN is true, thus NaN != NaN is false
- Complementary to float4_eq function but with inverted logic for NaN handling
- Used primarily in SQL inequality operations for real/float4 data types
- The function is located at src/include/utils/float.h:274-279

## Simplified Source

```c
static inline bool
float4_ne(const float4 val1, const float4 val2)
{
    // NaN-aware inequality: NaN != non-NaN is true, NaN != NaN is false
    return isnan(val1) ? !isnan(val2) : isnan(val2) || val1 != val2;
}
```