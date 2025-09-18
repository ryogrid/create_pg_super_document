# float8_ge

## Location
[src/include/utils/float.h:328-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L328-L333)

## Overview
Compares two double-precision floating-point numbers to determine if the first value is greater than or equal to the second, with special handling for NaN values.

## Definition


## Detailed Description
This inline function implements the greater-than-or-equal comparison operation for double-precision floating-point numbers (float8) with PostgreSQL's NaN handling semantics. Following SQL standards and IEEE 754 conventions used by PostgreSQL, any comparison involving NaN returns true when NaN is the first operand, implementing the behavior where NaN is considered greater than any non-NaN value including positive infinity.

The function returns true if val1 is NaN, or if val2 is not NaN and val1 is arithmetically greater than or equal to val2.

## Parameters / Member Variables
- : The first double-precision floating-point value (left operand of the comparison)
- : The second double-precision floating-point value (right operand of the comparison)

## Dependencies
- Functions called/Symbols referenced:
  - isnan (for NaN detection)
  - float4 (related floating-point type)
- Called from (representative examples):
  - [float8ge](float8ge.md) (SQL function wrapper)
  - [float48ge](float48ge.md) (mixed precision comparison)
  - [float84ge](float84ge.md) (mixed precision comparison)
  - [gist_box_picksplit](../g/gist_box_picksplit.md) (GiST index operations)
  - PLACE_RIGHT (geometric operations)

## Notes and Other Information
- This is an inline function defined in the header for performance
- Implements PostgreSQL's specific NaN semantics where NaN > any non-NaN value
- Part of the float8 family of comparison functions
- Used extensively in geometric indexing operations and SQL comparison functions
- The NaN handling ensures consistent behavior across PostgreSQL's floating-point operations