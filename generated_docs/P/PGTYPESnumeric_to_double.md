# PGTYPESnumeric_to_double

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 1483 - 1493

## Overview
Public API function that converts a PostgreSQL numeric value to a double-precision floating-point number, serving as a wrapper around the internal conversion logic.

## Definition
```c
int PGTYPESnumeric_to_double(numeric *nv, double *dp)
```

## Detailed Description
This function provides the public interface for converting PostgreSQL numeric values to double-precision floating-point numbers. It acts as a simple wrapper around the internal numericvar_to_double function, providing a clean API for external callers while delegating the actual conversion logic to the internal implementation. The function validates the conversion result and ensures the output parameter is properly set only on successful conversion.

The function performs:
1. Delegation to the internal numericvar_to_double function
2. Error propagation from the internal conversion
3. Output parameter assignment only on successful conversion
4. Return value handling to indicate success or failure

## Parameters / Member Variables
- `nv`: Pointer to the source numeric value to convert
- `dp`: Pointer to the double variable that will receive the converted value

## Dependencies
- Functions called/Symbols referenced:
  - [numericvar_to_double](../n/numericvar_to_double.md) (internal conversion function)
  - [numeric](../n/numeric.md) (type definition)
- Called from (representative examples):
  - [dectodbl](../d/dectodbl.md) (in informix compatibility library)
  - [main](../m/main.md) (in test programs for numeric functionality)
  - decimal (in test programs)

## Notes and Other Information
- Returns 0 on success, -1 on failure (with errno set by the internal function)
- Public API function in the ECPG pgtypes library
- Provides a simple, clean interface for numeric to double conversion
- Error handling and detailed conversion logic are delegated to numericvar_to_double
- Part of the standard type conversion functions in PostgreSQL's ECPG embedded C interface
- Commonly used in applications that need to convert PostgreSQL numeric values to C double values for mathematical operations