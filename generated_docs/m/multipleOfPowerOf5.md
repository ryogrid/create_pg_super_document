# multipleOfPowerOf5

## Location
[src/common/f2s.c:102-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L102-L108)

## Overview
Determines whether a given 64-bit unsigned integer value is divisible by a specified power of 5 (5^p).

## Definition

```c
static inline bool
multipleOfPowerOf5(const uint32 value, const uint32 p)
```
## Detailed Description
This function checks if a value is divisible by 5 raised to the power p (5^p) by leveraging the  function. It returns true if the highest power of 5 that divides the value is greater than or equal to the specified power p. The implementation is straightforward and efficient, avoiding complex case distinctions that showed no performance benefit in testing.

This function is commonly used in floating-point to decimal string conversion algorithms where determining divisibility by powers of 5 is crucial for optimizing decimal representation and rounding decisions.

## Parameters / Member Variables
- `value`: The 64-bit unsigned integer to test for divisibility
- `p`: The power of 5 (5^p) to test divisibility against
## Dependencies
- Functions called/Symbols referenced:
  - : Calculates the highest power of 5 that divides the value
- Called from (representative examples):
  -  (multiple calls in src/common/d2s.c:417, 427, 432)
  -  (multiple calls in src/common/f2s.c:298, 302, 306)

## Notes and Other Information
- Function is marked as  for performance optimization
- The author's comment indicates that case distinction on parameter p was tested but provided no performance improvement
- This is a utility function in PostgreSQL's decimal conversion implementation
- Returns a boolean result making it suitable for conditional logic in conversion algorithms
- Located in src/common/d2s.c:95-105