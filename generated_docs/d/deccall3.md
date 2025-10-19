# deccall3

## Location
[src/interfaces/ecpg/compatlib/informix.c:86-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L86-L150)

## Overview  
A static utility function that wraps numeric functions taking two input parameters and producing one output result, with comprehensive null handling and memory management for decimal-to-numeric conversions.

## Definition
```c
static int deccall3(decimal *arg1, decimal *arg2, decimal *result, int (*ptr)(numeric *, numeric *, numeric *))
```

## Detailed Description
The `deccall3` function is an internal helper in the ECPG Informix compatibility library that standardizes calls to numeric functions requiring two inputs and one output. It performs null checking on input arguments, converts Informix decimal types to PostgreSQL numeric types, executes the provided function, and converts the result back to decimal format. The function implements careful memory management and handles the case where the result variable might be the same as one of the input arguments.

## Parameters / Member Variables
- `arg1`: Pointer to the first decimal input argument
- `arg2`: Pointer to the second decimal input argument  
- `result`: Pointer to the decimal variable that will receive the result
- `ptr`: Function pointer to the numeric function that performs the actual operation

## Dependencies
- Functions called/Symbols referenced:
  - [risnull](../r/risnull.md)
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
  - [PGTYPESnumeric_from_decimal](../P/PGTYPESnumeric_from_decimal.md)
  - [PGTYPESnumeric_to_decimal](../P/PGTYPESnumeric_to_decimal.md)
  - [rsetnull](../r/rsetnull.md)
  - CDECIMALTYPE
  - ECPG_INFORMIX_OUT_OF_MEMORY
- Called from (representative examples):
  - [decadd](decadd.md)
  - [decdiv](decdiv.md)
  - [decmul](decmul.md)
  - [decsub](decsub.md)

## Notes and Other Information
- Static function internal to the Informix compatibility library
- Returns 0 immediately if either input argument is null
- Sets result to null before attempting conversion to handle potential errors
- Properly handles the case where result variable is aliased with input arguments
- Comprehensive memory cleanup on all error paths
- Used by arithmetic operations like addition, subtraction, multiplication, and division

## Simplified Source

```c
static int deccall3(decimal *arg1, decimal *arg2, decimal *result,
                   int (*ptr)(numeric *, numeric *, numeric *)) {
    numeric *a1, *a2, *nres;

    // Check for null inputs (return 0 if either is null)
    if (risnull(CDECIMALTYPE, (char *) arg1) || risnull(CDECIMALTYPE, (char *) arg2)) {
        return 0;
    }

    // Allocate numeric values for conversion
    if ((a1 = PGTYPESnumeric_new()) == NULL) return ECPG_INFORMIX_OUT_OF_MEMORY;
    if ((a2 = PGTYPESnumeric_new()) == NULL) {
        PGTYPESnumeric_free(a1);
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }
    if ((nres = PGTYPESnumeric_new()) == NULL) {
        PGTYPESnumeric_free(a1);
        PGTYPESnumeric_free(a2);
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }

    // Convert decimal inputs to numeric
    if (PGTYPESnumeric_from_decimal(arg1, a1) != 0 ||
        PGTYPESnumeric_from_decimal(arg2, a2) != 0) {
        PGTYPESnumeric_free(a1);
        PGTYPESnumeric_free(a2);
        PGTYPESnumeric_free(nres);
        return ECPG_INFORMIX_OUT_OF_MEMORY;
    }

    // Call the target function
    int ret = (*ptr)(a1, a2, nres);

    // Convert result back to decimal if successful
    if (ret == 0) {
        rsetnull(CDECIMALTYPE, (char *) result);  // Handle potential conversion errors
        PGTYPESnumeric_to_decimal(nres, result);
    }

    // Clean up all allocated memory
    PGTYPESnumeric_free(nres);
    PGTYPESnumeric_free(a1);
    PGTYPESnumeric_free(a2);

    return ret;
}
```