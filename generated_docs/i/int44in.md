# int44in

## Location
[src/test/regress/regress.c:502-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L502-L525)

## Overview
A PostgreSQL test function that parses a string representation of four comma-separated integers into an internal int32 array format.

## Definition

```c
Datum
int44in(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a custom input function for a test data type that represents an array of four 32-bit integers. It parses a string in the format "int1, int2, int3, int4" and converts it into an internal representation as a palloc'd array of int32 values. This function is part of PostgreSQL's regression test suite and demonstrates how to implement custom input functions for user-defined data types. If fewer than four integers are provided in the input string, the remaining positions are filled with zeros.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function call context and arguments
- : The input C-string containing comma-separated integers to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract C-string argument from function call
  - : PostgreSQL memory allocation function
  - : Standard C library function for formatted string parsing
  - : Macro to return a pointer value from PostgreSQL function
- Called from (representative examples):
  - : Referenced in the same test regression file

## Notes and Other Information
- This is a test function located in the PostgreSQL regression test suite
- The function allocates memory for exactly 4 int32 values regardless of input
- Missing values in the input string are automatically filled with zeros
- The function follows PostgreSQL's V1 calling convention for user-defined functions
- Part of a custom data type implementation for testing purposes