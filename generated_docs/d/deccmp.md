# deccmp

## Location
[src/interfaces/ecpg/compatlib/informix.c:167-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L167-L172)

## Overview
Compares two decimal numbers and returns an integer indicating their relative ordering, providing Informix-compatible decimal comparison functionality.

## Definition
```c
int deccmp(decimal *arg1, decimal *arg2)
```

## Detailed Description
The `deccmp` function implements decimal comparison for Informix compatibility in ECPG. It serves as a simple wrapper around the `deccall2` helper function, which handles the conversion to numeric types and calls `PGTYPESnumeric_cmp` to perform the actual comparison. The function returns standard comparison semantics: negative for less than, zero for equal, and positive for greater than.

## Parameters / Member Variables
- `arg1`: Pointer to the first decimal operand for comparison
- `arg2`: Pointer to the second decimal operand for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [deccall2](deccall2.md)
  - [PGTYPESnumeric_cmp](../P/PGTYPESnumeric_cmp.md)
- Called from (representative examples):
  - [main](../m/main.md) (in test programs)
  - ECPG applications using Informix decimal compatibility

## Notes and Other Information
- Part of the public Informix decimal compatibility API
- Returns < 0 if arg1 < arg2, 0 if arg1 = arg2, > 0 if arg1 > arg2
- Memory management and null handling are handled by the `deccall2` wrapper
- Essential for implementing conditional logic and sorting with decimal values
- Provides seamless migration path for applications using Informix decimal comparisons

## Simplified Source

```c
int deccmp(decimal *arg1, decimal *arg2) {
    // Compare two decimal numbers using Informix-compatible semantics
    // Returns: <0 if arg1 < arg2, 0 if equal, >0 if arg1 > arg2
    return deccall2(arg1, arg2, PGTYPESnumeric_cmp);
}
```