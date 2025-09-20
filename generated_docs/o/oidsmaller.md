# oidsmaller

## Location
[src/backend/utils/adt/oid.c:335-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L335-L343)

## Overview
The  function returns the smaller of two OID (Object Identifier) values, implementing a minimum function for the OID data type.

## Definition

```c
Datum
oidsmaller(PG_FUNCTION_ARGS)
```
## Detailed Description
This function compares two OID values and returns the smaller one. It performs a numerical comparison between the two input OIDs and uses a ternary operator to select the minimum value. The function follows PostgreSQL's standard function calling convention and returns an OID result using the  macro. This function is typically used in SQL contexts where finding the minimum OID value is needed.

## Parameters / Member Variables
- : The first OID operand for comparison
- : The second OID operand for comparison

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for extracting OID arguments)
  -  (macro for returning OID results)
- Called from (representative examples):
  - SQL aggregate functions or expressions requiring minimum OID values
  - PostgreSQL's built-in function dispatch system

## Notes and Other Information
- This function is part of PostgreSQL's built-in function set for the OID data type
- The function is located in 
- OID comparisons are straightforward numerical comparisons since OIDs are unsigned integers
- The function name follows PostgreSQL's naming convention for utility functions on data types
- Useful for finding the minimum OID in queries or system catalog operations
- Complements the  function for min/max operations on OIDs