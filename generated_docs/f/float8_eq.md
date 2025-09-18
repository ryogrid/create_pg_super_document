# float8_eq

## Location
src/include/utils/float.h: 268 - 273

## Overview
Compares two double-precision floating-point numbers for equality, handling NaN values correctly according to PostgreSQL semantics.

## Definition
```c
static inline bool float8_eq(const float8 val1, const float8 val2)
```

## Detailed Description
This inline function implements equality comparison for double-precision floating-point numbers (float8) with proper NaN handling. The function follows IEEE 754 semantics where NaN values are considered equal to each other but not equal to any other value, including other NaN values in standard comparison operations. However, PostgreSQL adopts a different approach where NaN == NaN returns true for consistency in database operations.

The implementation uses a conditional expression that first checks if val1 is NaN - if so, it returns whether val2 is also NaN. If val1 is not NaN, it ensures val2 is also not NaN before performing the standard equality comparison.

## Parameters / Member Variables
- `val1`: First double-precision floating-point value to compare
- `val2`: Second double-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - isnan (standard C library function for NaN detection)
- Called from (representative examples):
  - [float8eq](float8eq.md) (SQL-callable equality function)
  - [float48eq](float48eq.md) (mixed precision equality function)
  - [float84eq](float84eq.md) (mixed precision equality function) 
  - [gist_box_same](../g/gist_box_same.md) (GiST index operations)
  - [line_eq](../l/line_eq.md) (geometric line equality)
  - [point_eq_point](../p/point_eq_point.md) (geometric point equality)

## Notes and Other Information
- This is an inline function defined in utils/float.h for performance
- Handles the PostgreSQL-specific NaN semantics where NaN == NaN is true
- Used extensively in geometric operations and numeric comparisons throughout the system
- The function is located at src/include/utils/float.h:268-273