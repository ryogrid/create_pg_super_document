# float8_ne

## Location
[src/include/utils/float.h:280-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L280-L285)

## Overview
Compares two double-precision floating-point numbers for inequality, handling NaN values correctly according to PostgreSQL semantics.

## Definition
```c
static inline bool float8_ne(const float8 val1, const float8 val2)
```

## Detailed Description
This inline function implements inequality comparison for double-precision floating-point numbers (float8) with proper NaN handling. The function follows PostgreSQL semantics where NaN values are considered equal to each other but not equal to any other value. This means two NaN values are not considered "not equal" to each other, but a NaN is "not equal" to any non-NaN value.

The implementation uses a conditional expression that first checks if val1 is NaN - if so, it returns whether val2 is NOT NaN (since NaN != non-NaN is true, but NaN != NaN is false). If val1 is not NaN, it returns true if val2 is NaN or if the values are different using standard comparison.

## Parameters / Member Variables
- `val1`: First double-precision floating-point value to compare
- `val2`: Second double-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - isnan (standard C library function for NaN detection)
- Called from (representative examples):
  - [float8ne](float8ne.md) (SQL-callable inequality function)
  - [float48ne](float48ne.md) (mixed precision inequality function)
  - [float84ne](float84ne.md) (mixed precision inequality function)

## Notes and Other Information
- This is an inline function defined in utils/float.h for performance
- Handles the PostgreSQL-specific NaN semantics where NaN == NaN is true, thus NaN != NaN is false
- Complementary to float8_eq function but with inverted logic for NaN handling
- Used extensively in SQL inequality operations for double precision/float8 data types
- The function is located at src/include/utils/float.h:280-285

## Simplified Source

```c
static inline bool
float8_ne(const float8 val1, const float8 val2)
{
    // NaN-aware inequality: NaN != non-NaN is true, NaN != NaN is false
    return isnan(val1) ? !isnan(val2) : isnan(val2) || val1 != val2;
}
```