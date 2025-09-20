# multipleOfPowerOf2

## Location
[src/common/f2s.c:109-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L109-L119)

## Overview
Determines whether a given 64-bit unsigned integer value is divisible by a specified power of 2 (2^p).

## Definition

```c
static inline bool
multipleOfPowerOf2(const uint32 value, const uint32 p)
```
## Detailed Description
This function efficiently checks if a value is divisible by 2 raised to the power p (2^p) using bitwise operations. The implementation uses a mask-based approach: it creates a mask with the lower p bits set to 1, then performs a bitwise AND with the input value. If the result is zero, the value is divisible by 2^p.

The function includes a commented alternative implementation using  (count trailing zeros), which would also work but the current bitwise implementation was chosen. This is a fundamental utility in floating-point to decimal conversion where determining divisibility by powers of 2 is essential for binary representation analysis.

## Parameters / Member Variables
- : The 64-bit unsigned integer to test for divisibility
- : The power of 2 (2^p) to test divisibility against

## Dependencies
- Functions called/Symbols referenced:
  - : Macro for creating 64-bit constants
  - : Preprocessor conditional (referenced at line 158)
- Called from (representative examples):
  -  (in src/common/d2s.c:488)
  -  (in src/common/f2s.c:356)

## Notes and Other Information
- Function is marked as  for performance optimization
- Uses efficient bitwise operations instead of division or modulo operations
- The commented line shows an alternative implementation using compiler builtin function
- The mask  creates a value with the lower p bits set to 1
- This is a core utility function in PostgreSQL's floating-point conversion algorithms
- Located in src/common/d2s.c:106-161 (though the actual function body is much shorter)