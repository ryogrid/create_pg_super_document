# setDoubleValue

## Location
[src/bin/pgbench/pgbench.c:2118-2124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2118-L2124)

## Overview
A utility function that sets a PgBenchValue structure to hold a double-precision floating-point value.

## Definition

```c
static void
setDoubleValue(PgBenchValue *pv, double dval)
```
## Detailed Description
The  function is a simple utility function in pgbench that initializes a  structure to contain a double-precision floating-point value. It sets the type field to  and stores the provided double value in the appropriate union member. This function is part of pgbench's value handling system that supports different data types through a tagged union structure.

## Parameters / Member Variables
- : Pointer to a  structure that will be modified to hold the double value
- : The double-precision floating-point value to be stored in the structure

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type)
  -  (enum constant)
- Called from (representative examples):
  - 
  -  (multiple locations)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the pgbench.c source file
- Part of pgbench's typed value system that allows variables and expressions to hold different data types
- The function performs no validation on the input parameters - it assumes valid pointers and values
- Used extensively in mathematical function evaluation within pgbench expressions