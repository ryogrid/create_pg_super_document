# decadd

## Location
[src/interfaces/ecpg/compatlib/informix.c:151-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L151-L166)

## Overview
Performs addition of two decimal numbers, providing Informix-compatible decimal arithmetic with proper overflow/underflow handling and error reporting.

## Definition
```c
int decadd(decimal *arg1, decimal *arg2, decimal *sum)
```

## Detailed Description
The `decadd` function implements decimal addition for Informix compatibility in ECPG. It uses the internal `deccall3` helper function to perform the actual addition via `PGTYPESnumeric_add`, converting between decimal and numeric types as needed. The function provides comprehensive error handling for numeric overflow and underflow conditions, returning appropriate Informix-compatible error codes.

## Parameters / Member Variables
- `arg1`: Pointer to the first decimal operand
- `arg2`: Pointer to the second decimal operand
- `sum`: Pointer to the decimal variable that will store the addition result

## Dependencies
- Functions called/Symbols referenced:
  - [deccall3](deccall3.md)
  - [PGTYPESnumeric_add](../P/PGTYPESnumeric_add.md)
  - PGTYPES_NUM_OVERFLOW
  - PGTYPES_NUM_UNDERFLOW
  - ECPG_INFORMIX_NUM_OVERFLOW
  - ECPG_INFORMIX_NUM_UNDERFLOW
- Called from (representative examples):
  - [main](../m/main.md) (in test programs)
  - ECPG applications using Informix decimal compatibility

## Notes and Other Information
- Part of the public Informix decimal compatibility API
- Returns 0 on success, positive error codes for overflow/underflow, -1 for other errors
- Automatically handles null input checking through the `deccall3` wrapper
- Uses errno to detect specific numeric operation errors
- Essential function for applications porting from Informix to PostgreSQL

## Simplified Source

```c
int decadd(decimal *arg1, decimal *arg2, decimal *sum) {
    // Clear error status before operation
    errno = 0;

    // Perform decimal addition using the internal helper
    deccall3(arg1, arg2, sum, PGTYPESnumeric_add);

    // Check for specific numeric errors and return appropriate codes
    if (errno == PGTYPES_NUM_OVERFLOW) {
        return ECPG_INFORMIX_NUM_OVERFLOW;
    } else if (errno == PGTYPES_NUM_UNDERFLOW) {
        return ECPG_INFORMIX_NUM_UNDERFLOW;
    } else if (errno != 0) {
        return -1;  // General error
    } else {
        return 0;   // Success
    }
}
```