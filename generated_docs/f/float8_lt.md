# float8_lt

## Location
[src/include/utils/float.h:292-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L292-L297)

## Overview
Compares two double-precision floating-point numbers to determine if the first is less than the second, handling NaN values according to PostgreSQL semantics.

## Definition
```c
static inline bool float8_lt(const float8 val1, const float8 val2)
```

## Detailed Description
This inline function implements less-than comparison for double-precision floating-point numbers (float8) with proper NaN handling. The function follows PostgreSQL semantics for ordering comparisons where NaN is considered greater than any non-NaN value. This means any comparison where val1 is NaN returns false (NaN is not less than anything), while any non-NaN value is considered less than NaN.

The implementation first checks that val1 is not NaN (since NaN cannot be less than anything). If val1 is not NaN, it returns true if val2 is NaN (non-NaN < NaN) or if the standard comparison val1 < val2 is true.

## Parameters / Member Variables
- `val1`: First double-precision floating-point value (left operand)
- `val2`: Second double-precision floating-point value (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - isnan (standard C library function for NaN detection)
- Called from (representative examples):
  - [float8lt](float8lt.md) (SQL-callable less-than function)
  - [float8smaller](float8smaller.md) (minimum value function)
  - [float8_cmp_internal](float8_cmp_internal.md) (comparison utility function)
  - [float48lt](float48lt.md) (mixed precision less-than function)
  - [float84lt](float84lt.md) (mixed precision less-than function)
  - [adjustBox](../a/adjustBox.md) (GiST index operations)
  - [gist_box_picksplit](../g/gist_box_picksplit.md) (GiST index operations)
  - Various geometric functions (box_in, path_distance, etc.)
  - [float8_min](float8_min.md) (inline minimum function)

## Notes and Other Information
- This is an inline function defined in utils/float.h for performance
- Implements PostgreSQL semantics where NaN > any non-NaN value in ordering
- Used extensively in SQL comparison operations, aggregate functions, and geometric calculations
- Part of the consistent family of float8 comparison functions
- Widely used throughout the system for both numeric and geometric operations
- The function is located at src/include/utils/float.h:292-297