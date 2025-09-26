# Complex

## Location
[src/tutorial/complex.c:17-21](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/complex.c#L17-L21)

## Overview
Complex is a custom data type structure representing complex numbers in PostgreSQL's tutorial extension, designed to demonstrate how to create user-defined data types with complete input/output functionality and operator support.

## Definition

```c
typedef struct Complex
{
	double		x;
	double		y;
}			Complex;
```
## Detailed Description
The Complex structure is part of PostgreSQL's tutorial system located in src/tutorial/complex.c, serving as an educational example of how to implement a complete user-defined data type. This structure represents complex numbers with real and imaginary components stored as double-precision floating-point values. The implementation includes comprehensive functionality such as text input/output, binary serialization/deserialization, arithmetic operations, and comparison operators suitable for B-tree indexing.

The design follows PostgreSQL's extension architecture patterns, providing all necessary functions for the type to be fully integrated into the database system. It demonstrates key concepts including memory allocation using palloc(), error handling with ereport(), and the PostgreSQL function calling convention using PG_FUNCTION_ARGS and related macros.

## Parameters / Member Variables
- : The real component of the complex number, stored as a double-precision floating-point value
- : The imaginary component of the complex number, stored as a double-precision floating-point value

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references - this is a simple struct definition)
- Called from (representative examples):
  - complex_in (input function)
  - complex_out (output function)
  - complex_recv (binary input function)
  - complex_send (binary output function)
  - complex_add (addition operator)
  - complex_abs_lt, complex_abs_le, complex_abs_eq, complex_abs_ge, complex_abs_gt (comparison operators)
  - complex_abs_cmp (comparison function for B-tree support)

## Notes and Other Information
- This is a tutorial/educational implementation demonstrating PostgreSQL's extensibility features
- The structure uses PostgreSQL's standard double type for both components, ensuring platform compatibility
- Memory management for Complex instances uses PostgreSQL's memory context system (palloc/pfree)
- The comparison operators are based on magnitude comparison (|a| vs |b|) rather than lexicographic ordering
- Includes complete B-tree operator class support with consistent three-way comparison semantics
- Located in src/tutorial/complex.c:17-21
- Serves as a reference implementation for developers creating custom PostgreSQL data types