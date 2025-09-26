# int64_multiply_add

## Location
src/backend/utils/adt/datetime.c: 522 - 536

## Overview
int64_multiply_add is a utility function that safely performs multiplication and addition operations on 64-bit integers with overflow detection and prevention.

## Definition

```c
static bool
int64_multiply_add(int64 val, int64 multiplier, int64 *sum)
```
## Detailed Description
int64_multiply_add implements a safe arithmetic operation that computes `val * multiplier + *sum` and stores the result back in `*sum`. The function provides overflow protection by using PostgreSQL's safe arithmetic functions that detect overflow conditions.

The operation is performed in two stages:
1. **Multiplication with overflow check**: Uses pg_mul_s64_overflow() to safely multiply val by multiplier
2. **Addition with overflow check**: Uses pg_add_s64_overflow() to safely add the product to the current sum

If either operation would cause an overflow, the function returns false and leaves the sum unchanged, ensuring data integrity and preventing undefined behavior.

## Parameters / Member Variables
- `val`: The first operand for multiplication (64-bit integer)
- `multiplier`: The second operand for multiplication (64-bit integer)  
- `sum`: Pointer to the accumulator value; receives the final result if successful

## Dependencies
- Functions called/Symbols referenced:
  - pg_mul_s64_overflow (safe 64-bit multiplication with overflow detection)
  - pg_add_s64_overflow (safe 64-bit addition with overflow detection)
- Called from (representative examples):
  - AdjustMicroseconds
  - DecodeTimeForInterval (multiple times for different time units)

## Notes and Other Information
- Returns true on successful operation, false if overflow would occur
- The sum parameter serves as both input and output - it's modified only on success
- Part of PostgreSQL's safe arithmetic infrastructure used in time/date calculations
- Commonly used in interval and timestamp processing where large time values might cause overflow
- The function ensures that if overflow is detected at any stage, the original sum value remains unchanged
- Used extensively in time unit conversions where multiplication by large factors (like microseconds per day) is common