# decmul

## Location
[src/interfaces/ecpg/compatlib/informix.c:337-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L337-L358)

## Overview
Performs multiplication operation on two decimal numbers using ECPG Informix compatibility library.

## Definition
```c
int decmul(decimal *n1, decimal *n2, decimal *result)
```

## Detailed Description
The `decmul` function multiplies two decimal numbers (`n1` * `n2`) and stores the result in the `result` parameter. This function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility library, providing compatibility with Informix database decimal arithmetic operations. The function internally uses `deccall3` helper function with `PGTYPESnumeric_mul` to perform the actual multiplication operation and handles overflow and underflow conditions that may occur during multiplication.

## Parameters / Member Variables
- `n1`: Pointer to the first decimal multiplicand
- `n2`: Pointer to the second decimal multiplicand
- `result`: Pointer to the decimal where the multiplication result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [deccall3](deccall3.md)
  - [PGTYPESnumeric_mul](../P/PGTYPESnumeric_mul.md)
- Called from (representative examples):
  - [main](../m/main.md) (in test files)
- Error constants used:
  - PGTYPES_NUM_OVERFLOW
  - ECPG_INFORMIX_NUM_OVERFLOW
  - ECPG_INFORMIX_NUM_UNDERFLOW

## Notes and Other Information
- Returns 0 on success
- Returns specific error codes for different failure conditions:
  - ECPG_INFORMIX_NUM_OVERFLOW when result overflows
  - ECPG_INFORMIX_NUM_UNDERFLOW for other numeric errors
- Sets errno internally to communicate error conditions to the caller
- Located in src/interfaces/ecpg/compatlib/informix.c:337-358
- Unlike division, multiplication does not need to handle divide-by-zero errors

## Simplified Source

```c
int decmul(decimal *n1, decimal *n2, decimal *result) {
    // Clear errno and perform multiplication using helper function
    errno = 0;
    int i = deccall3(n1, n2, result, PGTYPESnumeric_mul);

    // Handle errors if multiplication failed
    if (i != 0) {
        switch (errno) {
            case PGTYPES_NUM_OVERFLOW:
                return ECPG_INFORMIX_NUM_OVERFLOW;
            default:
                return ECPG_INFORMIX_NUM_UNDERFLOW;
        }
    }

    return 0;  // Success
}
```