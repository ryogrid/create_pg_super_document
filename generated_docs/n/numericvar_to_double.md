# numericvar_to_double

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:1432-1482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L1432-L1482)

## Overview
Internal static function that converts a PostgreSQL numeric value to a double-precision floating-point number using string conversion with proper error handling.

## Definition
```c
static int numericvar_to_double(numeric *var, double *dp)
```

## Detailed Description
This internal function converts a numeric value to a double by first creating a working copy of the input numeric, converting it to its string representation, and then parsing that string using the standard C library strtod function. The function includes comprehensive error handling for overflow, underflow, and malformed numeric values. It uses a copy of the input to avoid modifying the original numeric value during the conversion process.

The conversion process involves:
1. Creating a working copy of the input numeric value
2. Converting the numeric copy to its string representation with appropriate decimal scale
3. Using strtod to parse the string into a double value
4. Handling various error conditions (range errors, parsing errors)
5. Setting appropriate errno values for different failure modes
6. Cleaning up allocated memory and returning the result

## Parameters / Member Variables
- `var`: Pointer to the source numeric value to convert
- `dp`: Pointer to the double variable that will receive the converted value

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md) (creates new numeric variable)
  - [PGTYPESnumeric_copy](../P/PGTYPESnumeric_copy.md) (copies numeric values)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md) (frees numeric memory)
  - [get_str_from_var](../g/get_str_from_var.md) (converts numeric to string representation)
  - PGTYPES_NUM_UNDERFLOW (error constant for underflow)
  - PGTYPES_NUM_OVERFLOW (error constant for overflow)
  - PGTYPES_NUM_BAD_NUMERIC (error constant for bad numeric)
- Called from (representative examples):
  - [PGTYPESnumeric_to_double](../P/PGTYPESnumeric_to_double.md) (public wrapper function)

## Notes and Other Information
- Returns 0 on success, -1 on failure with appropriate errno set
- Static function - internal implementation detail not exposed in public API
- Creates a working copy to preserve the original numeric value
- Uses strtod for the actual conversion, providing standard C library compatibility
- Comprehensive error handling covers overflow, underflow, and parsing errors
- Sets specific PGTYPES error codes to distinguish different failure modes
- Properly manages memory allocation and cleanup throughout the conversion process
- Part of the ECPG pgtypes library's internal numeric conversion infrastructure

## Simplified Source

```c
static int
numericvar_to_double(numeric *var, double *dp)
{
    // Create working copy to preserve original
    numeric *varcopy = PGTYPESnumeric_new();
    if (varcopy == NULL)
        return -1;

    if (PGTYPESnumeric_copy(var, varcopy) < 0) {
        PGTYPESnumeric_free(varcopy);
        return -1;
    }

    // Convert numeric to string representation
    char *str = get_str_from_var(varcopy, varcopy->dscale);
    PGTYPESnumeric_free(varcopy);
    if (str == NULL)
        return -1;

    // Parse string to double using strtod
    errno = 0;
    char *endptr;
    double val = strtod(str, &endptr);

    // Check for conversion errors
    if (errno == ERANGE) {
        free(str);
        errno = (val == 0) ? PGTYPES_NUM_UNDERFLOW : PGTYPES_NUM_OVERFLOW;
        return -1;
    }

    if (*endptr != '\0') {
        free(str);
        errno = PGTYPES_NUM_BAD_NUMERIC;
        return -1;
    }

    // Success: store result and cleanup
    free(str);
    *dp = val;
    return 0;
}
```

**Key Points:**
- Creates a copy to avoid modifying the original numeric value
- Converts numeric → string → double using standard C library
- Comprehensive error handling for overflow, underflow, and parsing errors
- Returns 0 on success, -1 on failure with specific errno codes