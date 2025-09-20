# print_double

## Location
[src/interfaces/ecpg/test/expected/pgtypeslib-num_test2.c:34-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/pgtypeslib-num_test2.c#L34-L77)

## Overview
A utility function that provides cross-platform consistent formatting when printing double precision floating-point numbers, designed to normalize the output format across different operating systems.

## Definition

```c
static void
print_double(double x)
```
## Detailed Description
The  function serves as a cross-platform wrapper around  to ensure consistent formatting of double precision numbers across different platforms. The function specifically addresses formatting differences on Windows, where exponents are displayed with 3 digits instead of the standard 2-digit format used on other platforms.

On Windows systems, the function captures the  output in a temporary buffer and reformats 3-digit exponents (e.g., "e+001") to 2-digit format (e.g., "e+01") by removing the leading zero from the exponent. On non-Windows platforms, it simply calls  directly.

## Parameters / Member Variables
- : A double precision floating-point number to be printed with normalized formatting

## Dependencies
- Functions called/Symbols referenced:
  - sprintf (Windows only)
  - strlen (Windows only) 
  - printf
  - [check_errno](../c/check_errno.md)
- Called from (representative examples):
  - [main](../m/main.md) (in compat_informix-dec_test.c)
  - [main](../m/main.md) (in pgtypeslib-num_test.c)
  - [main](../m/main.md) (in pgtypeslib-num_test2.c)

## Notes and Other Information
- This function is marked as , meaning it has internal linkage and is only visible within its compilation unit
- The function is primarily used in PostgreSQL's ECPG (Embedded SQL in C) test suite
- The Windows-specific formatting logic handles the conversion of 3-digit exponents by shifting characters in the string buffer
- The function ensures consistent test output across platforms, which is crucial for automated testing frameworks