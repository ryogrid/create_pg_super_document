# pg_sub_u64_overflow

## Location
src/include/common/int.h: 398 - 413

## Overview
A safe unsigned 64-bit integer subtraction function that detects arithmetic underflow, preventing wraparound behavior that can occur with unsigned integer subtraction.

## Definition
```c
static inline bool pg_sub_u64_overflow(uint64 a, uint64 b, uint64 *result)
```

## Detailed Description
This inline function performs subtraction of two 64-bit unsigned integers with underflow detection. It returns a boolean indicating whether underflow occurred during the operation (when the second operand is larger than the first). The function leverages compiler built-ins when available (`__builtin_sub_overflow`) for optimal performance and accuracy, falling back to a manual underflow check on systems without built-in support.

When underflow is detected in the fallback implementation, the function sets a dummy value (0x5EED) to the result pointer to avoid spurious compiler warnings about uninitialized variables, though the actual result should not be used when underflow occurs.

## Parameters / Member Variables
- `a`: The minuend (value being subtracted from) - 64-bit unsigned integer
- `b`: The subtrahend (value being subtracted) - 64-bit unsigned integer  
- `result`: Pointer to store the subtraction result (valid only when no underflow occurs)

## Dependencies
- Functions called/Symbols referenced:
  - `__builtin_sub_overflow` (compiler built-in, when available)
- Called from (representative examples):
  - No current references found in the codebase

## Notes and Other Information
- The function uses conditional compilation with `HAVE__BUILTIN_OP_OVERFLOW` to choose between compiler built-in and manual underflow detection
- In the manual implementation, underflow is detected by checking if the subtrahend is greater than the minuend
- The dummy value 0x5EED is used to prevent compiler warnings about uninitialized memory when underflow occurs
- This function is part of PostgreSQL's safe arithmetic operations suite for preventing integer overflow/underflow vulnerabilities
- For unsigned integers, underflow occurs when subtracting a larger value from a smaller one, which would normally wrap around to a very large positive value