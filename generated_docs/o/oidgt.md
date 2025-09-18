# oidgt

## Location
[src/backend/utils/adt/oid.c:317-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L317-L325)

## Overview
The  function implements the greater-than comparison operator for PostgreSQL's OID (Object Identifier) data type.

## Definition


## Detailed Description
This function provides the SQL operator  for OID values in PostgreSQL. It extracts two OID arguments from the function call context and performs a numerical comparison to determine if the first OID is strictly greater than the second OID. The function follows PostgreSQL's standard function calling convention using the  macro and returns a boolean result using .

## Parameters / Member Variables
- : The first OID operand (left side of the  operator)
- : The second OID operand (right side of the  operator)

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for extracting OID arguments)
  -  (macro for returning boolean results)
- Called from (representative examples):
  - SQL queries using  operator on OID columns
  - PostgreSQL's operator dispatch system

## Notes and Other Information
- This function is part of PostgreSQL's built-in operator set for the OID data type
- The function is located in 
- OID comparisons are straightforward numerical comparisons since OIDs are unsigned integers
- The function name follows PostgreSQL's naming convention for comparison operators (oid + gt for 'greater than')