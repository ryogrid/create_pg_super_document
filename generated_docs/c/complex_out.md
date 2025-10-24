# complex_out

## Location
[src/tutorial/complex.c:53-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/complex.c#L53-L70)

## Overview
Output function for the  data type that converts PostgreSQL's internal representation of a complex number to its external string format.

## Definition

```c
Datum
complex_out(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum complex_out(PG_FUNCTION_ARGS) {
    Complex *complex = (Complex *) PG_GETARG_POINTER(0);
    char *result;

    // Format as "(x,y)" without spaces
    result = psprintf("(%g,%g)", complex->x, complex->y);
    PG_RETURN_CSTRING(result);
}
```