# complex_add

## Location
[src/tutorial/complex.c:105-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/complex.c#L105-L128)

## Overview
Mathematical operation function for the  data type that performs addition of two complex numbers.

## Definition

```c
Datum
complex_add(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements complex number addition by taking two  structures as input and returning their sum. It performs component-wise addition where the real parts are added together and the imaginary parts are added together separately. This function demonstrates how to implement mathematical operations for custom PostgreSQL data types and can be used in SQL expressions and queries.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  -  (accessed via ): Pointer to the first Complex number operand
  -  (accessed via ): Pointer to the second Complex number operand

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract pointer arguments (used twice)
  - : PostgreSQL memory allocation function
  - : Macro to return pointer value
- Called from (representative examples):
  - : Referenced in the same file for function registration

## Notes and Other Information
- Implements mathematical formula: (a.x + b.x) + (a.y + b.y)i
- Allocates new memory for the result using PostgreSQL's palloc()
- Can be registered as an operator in PostgreSQL to enable usage with "+" symbol in SQL
- Part of the PostgreSQL tutorial demonstrating custom data type implementation with operations
- Serves as a template for implementing other mathematical operations (subtraction, multiplication, etc.)
- Located in src/tutorial/complex.c:105-128

## Simplified Source

```c
Datum complex_add(PG_FUNCTION_ARGS) {
    // Extract input complex numbers
    Complex *a = (Complex *) PG_GETARG_POINTER(0);
    Complex *b = (Complex *) PG_GETARG_POINTER(1);

    // Allocate result and perform component-wise addition
    Complex *result = (Complex *) palloc(sizeof(Complex));
    result->x = a->x + b->x;  // Add real parts
    result->y = a->y + b->y;  // Add imaginary parts

    PG_RETURN_POINTER(result);
}
```