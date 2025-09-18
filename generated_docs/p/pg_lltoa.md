# pg_lltoa

## Location
[src/backend/utils/adt/numutils.c:1229-1268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L1229-L1268)

## Overview
Converts a signed 64-bit integer to its null-terminated string representation and returns the length of the resulting string.

## Definition
```c
int pg_lltoa(int64 value, char *a)
```

## Detailed Description
The `pg_lltoa` function is a PostgreSQL utility function that converts a signed 64-bit integer (`int64`) to its decimal string representation with proper null termination. This function is the 64-bit equivalent of `pg_ltoa`, handling the full range of signed 64-bit integers from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.

Like its 32-bit counterpart, the implementation leverages the optimized `pg_ulltoa_n` function for the actual numeric conversion while adding the necessary logic for sign handling and null termination. For negative values, it converts them to their positive equivalents using two's complement arithmetic and prepends a minus sign at the beginning of the string.

The function provides a complete string conversion solution for 64-bit integers, making the result suitable for standard C string operations and PostgreSQL's bigint output functions.

## Parameters / Member Variables
- `value`: The signed 64-bit integer to convert to string representation  
- `a`: Pointer to the output buffer where the null-terminated string representation will be written (must have at least MAXINT8LEN + 1 bytes, typically 21 bytes: 1 for sign + 19 for digits + 1 for null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ulltoa_n](pg_ulltoa_n.md) (performs the actual unsigned 64-bit integer to string conversion)
- Called from (representative examples):
  - [printsimple](printsimple.md) (for debug output formatting of 64-bit values)
  - [int8out](../i/int8out.md) (converts int64 to string for PostgreSQL bigint output)

## Notes and Other Information
- The output string IS null-terminated, making it suitable for standard C string operations
- Requires at least MAXINT8LEN + 1 bytes of buffer space (typically 21 bytes for worst case)
- Handles the full range of 64-bit signed integers (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
- Uses efficient two's complement arithmetic for negative number conversion
- Returns string length, eliminating need for separate strlen() call
- Essential for PostgreSQL's bigint data type output and formatting
- Leverages the highly optimized pg_ulltoa_n implementation for performance
- Used in debug output and wherever 64-bit integer string representation is needed