# complex_out

## Location
src/tutorial/complex.c: 53 - 70

## Overview
Output function for the  data type that converts PostgreSQL's internal representation of a complex number to its external string format.

## Definition


## Detailed Description
The  function is responsible for converting PostgreSQL's internal  data type representation into a human-readable string format. It takes a Complex structure pointer and formats it as "(x,y)" where x and y are the real and imaginary parts respectively. This function is the counterpart to  and is used when PostgreSQL needs to display complex values to users or convert them for output.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  -  (accessed via ): Pointer to the Complex structure to be converted

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract pointer argument
  - : PostgreSQL's formatted string printing function
  - : Macro to return C string value
- Called from (representative examples):
  - : Referenced in the same file for function registration

## Notes and Other Information
- Output format: "(x,y)" without spaces around parentheses or comma (differs from input format)
- Uses  which automatically allocates memory for the result string
- The  format specifier automatically chooses between decimal and scientific notation
- Part of the PostgreSQL tutorial demonstrating custom data type implementation
- Located in src/tutorial/complex.c:53-70