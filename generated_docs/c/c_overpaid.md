# c_overpaid

## Location
[src/tutorial/funcs.c:110-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/funcs.c#L110-L126)

## Overview
A PostgreSQL C function that determines whether an employee's salary exceeds a specified limit, designed as a tutorial example for composite type handling.

## Definition


## Detailed Description
The  function is a PostgreSQL C language function that takes a composite type (employee record) and a salary limit as parameters, then returns a boolean indicating whether the employee's salary is greater than the specified limit. This function demonstrates how to extract attributes from composite types using PostgreSQL's internal API.

The function handles null salary values by returning false rather than null, though the code includes a comment noting that returning null for null salaries would be an alternative approach. This is part of PostgreSQL's tutorial code for demonstrating user-defined functions in C.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  - Argument 0:  - A composite type containing employee data (including salary field)
  - Argument 1:  - The salary limit threshold for comparison

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts the composite type from function arguments
  - : Extracts the integer limit from function arguments
  - : Retrieves the "salary" attribute from the composite type
  - : Converts the salary Datum to int32
  - : Returns a boolean result to the SQL engine
- Called from (representative examples):
  - Referenced in tutorial examples (exact calling context would depend on SQL usage)

## Notes and Other Information
- Located in  as part of PostgreSQL's tutorial code
- The function explicitly handles null salary values by returning false, but includes a comment suggesting that  could be used as an alternative
- This is a demonstration function showing proper techniques for working with composite types in PostgreSQL C functions
- The function uses PostgreSQL's standard macros for argument handling and return values, following established patterns for C language functions