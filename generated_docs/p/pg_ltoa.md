# pg_ltoa

## Location
src/backend/utils/adt/numutils.c: 1122 - 1141

## Overview
Converts a signed 32-bit integer to its null-terminated string representation and returns the length of the resulting string.

## Definition
```c
int pg_ltoa(int32 value, char *a)
```

## Detailed Description
The `pg_ltoa` function is a PostgreSQL utility function that converts a signed 32-bit integer (`int32`) to its decimal string representation with proper null termination. This function handles both positive and negative integers by converting negative values to their positive equivalents and prepending a minus sign.

The implementation leverages the optimized `pg_ultoa_n` function for the actual numeric conversion while adding the necessary logic for sign handling and null termination. For negative values, it converts them to their positive equivalents using two's complement arithmetic and adds the minus sign at the beginning of the string.

The function provides a complete string conversion solution, unlike `pg_ultoa_n` which does not null-terminate the output.

## Parameters / Member Variables
- `value`: The signed 32-bit integer to convert to string representation
- `a`: Pointer to the output buffer where the null-terminated string representation will be written (must have at least 12 bytes: 1 for sign + 10 for digits + 1 for null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - pg_ultoa_n (performs the actual unsigned integer to string conversion)
- Called from (representative examples):
  - printsimple (for debug output formatting)
  - int4out (converts int32 to string for PostgreSQL output)
  - executeItemOptUnwrapTarget (in JSON path execution contexts)
  - pg_itoa (uses pg_ltoa as implementation for 16-bit integers)

## Notes and Other Information
- The output string IS null-terminated, making it suitable for standard C string operations
- Requires at least 12 bytes of buffer space (worst case: "-2147483648" + null terminator)
- Handles the full range of 32-bit signed integers (-2,147,483,648 to 2,147,483,647)  
- Uses efficient two's complement arithmetic for negative number conversion
- Returns string length, eliminating need for separate strlen() call
- Serves as the foundation for smaller integer type conversions (via pg_itoa)
- Used extensively in PostgreSQL's integer output functions and internal formatting