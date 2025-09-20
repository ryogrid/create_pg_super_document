# float8eq

## Location
[src/backend/utils/adt/float.c:913-921](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L913-L921)

## Overview
PostgreSQL function that tests equality between two double-precision floating-point numbers (float8) and returns a boolean result.

## Definition

```c
Datum
float8eq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the SQL-callable equality comparison operator for double-precision floating-point numbers in PostgreSQL. It extracts two float8 arguments from the function call context and delegates the actual equality testing to the internal  function. The function returns a PostgreSQL boolean Datum indicating whether the two values are equal. This function serves as the implementation for the  operator when applied to float8 values in SQL queries, handling special floating-point cases such as NaN comparisons according to IEEE 754 standards through the underlying  function.

## Parameters / Member Variables
- Function uses  macro to access arguments:
  - First argument (index 0): First float8 value to compare
  - Second argument (index 1): Second float8 value to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract float8 arguments from function call
  - : Internal equality comparison function for float8 values
  - : Macro to return boolean result from PostgreSQL function
  - : Double-precision floating-point data type

- Called from (representative examples):
  - SQL queries using the  operator with float8 operands
  - PostgreSQL's operator dispatch system
  - Expression evaluation in query execution

## Notes and Other Information
- This function is part of PostgreSQL's operator system for float8 data types
- The actual equality logic is implemented in  for code reuse across different contexts
- Handles special floating-point cases like NaN according to SQL and IEEE 754 standards
- Located in 
- Returns a Datum-wrapped boolean value following PostgreSQL's function calling conventions
- Used in WHERE clauses, JOIN conditions, and other SQL constructs requiring float8 equality testing