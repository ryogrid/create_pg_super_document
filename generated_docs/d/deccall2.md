# deccall2

## Location
[src/interfaces/ecpg/compatlib/informix.c:48-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L48-L85)

## Overview
A static utility function that serves as a wrapper to call numeric functions that take two parameters and return an integer result, handling memory management and decimal-to-numeric conversion.

## Definition

```c
static int
deccall2(decimal *arg1, decimal *arg2, int (*ptr) (numeric *, numeric *))
```
## Detailed Description
The  function is an internal helper function in the ECPG Informix compatibility library that provides a standardized way to call numeric functions requiring two input parameters. It handles the conversion from Informix decimal types to PostgreSQL numeric types, manages memory allocation and cleanup, and executes the provided function pointer. The function ensures proper resource management by cleaning up allocated numeric values regardless of success or failure.

## Parameters / Member Variables
- `*arg1`: Pointer to the first decimal input argument to be converted to numeric type
- `*arg2`: Pointer to the second decimal input argument to be converted to numeric type
- `*ptr`: Function pointer to the numeric function to be called with the converted arguments
## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
  - [PGTYPESnumeric_from_decimal](../P/PGTYPESnumeric_from_decimal.md)
  - ECPG_INFORMIX_OUT_OF_MEMORY
- Called from (representative examples):
  - [deccmp](deccmp.md)

## Notes and Other Information
- This is a static function internal to the Informix compatibility library
- Provides consistent error handling for out-of-memory conditions
- Always cleans up allocated memory before returning
- Returns the result of the called function pointer or error codes on failure
- Part of the ECPG interface for Informix decimal type compatibility

## Simplified Source

```c
static int deccall2(decimal *arg1, decimal *arg2, int (*ptr)(numeric *, numeric *)) {
    numeric *a1, *a2;

    // Allocate numeric values
    if ((a1 = PGTYPESnumeric_new()) == NULL) {
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }
    if ((a2 = PGTYPESnumeric_new()) == NULL) {
        PGTYPESnumeric_free(a1);
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }

    // Convert decimal arguments to numeric
    if (PGTYPESnumeric_from_decimal(arg1, a1) != 0) {
        PGTYPESnumeric_free(a1);
        PGTYPESnumeric_free(a2);
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }
    if (PGTYPESnumeric_from_decimal(arg2, a2) != 0) {
        PGTYPESnumeric_free(a1);
        PGTYPESnumeric_free(a2);
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }

    // Call the target function with converted arguments
    int result = (*ptr)(a1, a2);

    // Clean up allocated memory
    PGTYPESnumeric_free(a1);
    PGTYPESnumeric_free(a2);

    return result;
}
```