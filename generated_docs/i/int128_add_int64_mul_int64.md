# int128_add_int64_mul_int64

## Location
[src/include/common/int128.h:62-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int128.h#L62-L70)

## Overview
Adds the 128-bit product of two signed 64-bit integers to an existing 128-bit integer variable in-place.

## Definition
```c
static inline void
int128_add_int64_mul_int64(INT128 *i128, int64 x, int64 y)
```

## Detailed Description
This function computes the product of two signed 64-bit integers (x * y) and adds the result to an existing 128-bit integer. The implementation varies depending on whether native 128-bit integer support is available:

1. **Native 128-bit implementation**: Uses direct casting and multiplication: `*i128 += (int128) x * (int128) y`
2. **Manual implementation**: When native 128-bit integers aren't available, it performs 64-bit arithmetic by breaking each input into 32-bit high and low parts and computing the cross products manually.

The function includes a compiler performance warning noting that with poor compiler optimization, the simple implementation might be less efficient than the manual approach.

## Parameters / Member Variables
- `i128`: Pointer to the INT128 variable that will be modified. The product x*y is added to this variable's current value.
- `x`: First signed 64-bit integer operand for multiplication.
- `y`: Second signed 64-bit integer operand for multiplication.

## Dependencies
- Functions called/Symbols referenced:
  - INT128 (type definition)
  - [int128_add_uint64](int128_add_uint64.md) (used in manual implementation for partial products)
- Called from (representative examples):
  - [interval_cmp_value](interval_cmp_value.md) (in src/backend/utils/adt/timestamp.c:2499)
  - [main](../m/main.md) (in src/tools/testint128.c:127)

## Notes and Other Information
- This is a static inline function with two conditional implementations based on USE_NATIVE_INT128
- The manual implementation uses INT64_AU32 and INT64_AL32 macros to extract high and low 32-bit parts
- Includes overflow-safe arithmetic by computing products in 64-bit and carefully managing sign extension
- The comment suggests potential compiler optimization concerns with the simple native implementation
- Used for complex arithmetic operations requiring 128-bit precision, such as timestamp calculations
- The manual implementation includes a static assertion to ensure arithmetic right shift behavior