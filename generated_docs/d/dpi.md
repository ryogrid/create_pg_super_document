# dpi

## Location
[src/backend/utils/adt/float.c:2566-2575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2566-L2575)

## Overview
The  function returns the mathematical constant π (pi), providing access to this fundamental mathematical value for trigonometric and geometric calculations.

## Definition

```c
Datum
dpi(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a simple accessor function that returns the mathematical constant π (pi). It uses the standard C library constant  which provides a high-precision representation of π ≈ 3.14159265358979323846. This function serves as a PostgreSQL SQL-callable interface to obtain the pi constant for mathematical computations, particularly useful in trigonometric functions, geometric calculations, and various mathematical operations that require π.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - M_PI: Standard C library mathematical constant for π
- Called from: No direct references found in the codebase

## Notes and Other Information
- Returns the mathematical constant π with high precision
- Uses the standard C library M_PI constant for consistency and accuracy
- No input validation needed as it returns a constant value
- Located in src/backend/utils/adt/float.c:2566-2575
- Useful for SQL queries requiring the pi constant for mathematical calculations