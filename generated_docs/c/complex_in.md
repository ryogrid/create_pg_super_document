# complex_in

## Location
[src/tutorial/complex.c:31-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/complex.c#L31-L52)

## Overview
Input function for the  data type that parses a string representation of a complex number and converts it to the internal PostgreSQL representation.

## Definition


## Detailed Description
The  function is responsible for converting external string representations of complex numbers into PostgreSQL's internal  data type format. It expects input in the form "(x, y)" where x and y are double-precision floating-point numbers representing the real and imaginary parts respectively. The function performs input validation and error reporting for malformed input strings.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  -  (accessed via ): Input string containing the complex number representation

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract C string argument
  - : Standard C library function for formatted input parsing
  - : PostgreSQL error reporting function
  - : PostgreSQL memory allocation function
  - : Macro to return pointer value
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Expects input format: "( x , y )" with spaces around parentheses and comma
- Returns ERROR with code ERRCODE_INVALID_TEXT_REPRESENTATION for malformed input
- Allocates memory using PostgreSQL's palloc() for the result structure
- Part of the PostgreSQL tutorial demonstrating custom data type implementation
- Located in src/tutorial/complex.c:31-52