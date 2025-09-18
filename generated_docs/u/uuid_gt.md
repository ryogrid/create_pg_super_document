# uuid_gt

## Location
[src/backend/utils/adt/uuid.c:210-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L210-L218)

## Overview
Implements the greater than comparison operator (>) for PostgreSQL UUID data type, returning true if the first UUID is lexicographically greater than the second UUID.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that compares two UUID values and returns a boolean result indicating whether the first UUID is greater than the second. The comparison is performed lexicographically using the internal  function. This function is typically used in WHERE clauses, ORDER BY statements, and other SQL constructs that require UUID comparison operations. The function follows PostgreSQL's standard function calling convention using the  macro and returns a  type.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (pg_uuid_t*): Pointer to the first UUID value to compare
  -  (pg_uuid_t*): Pointer to the second UUID value to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract UUID arguments from function call
  - : Internal function performing the actual UUID comparison
  - : Macro to return boolean result
  - : UUID data type structure
- Called from (representative examples):
  - SQL queries using UUID > operator
  - Index operations requiring UUID ordering
  - B-tree comparison operations

## Notes and Other Information
- The function returns true when 
- Part of the UUID data type operator family in PostgreSQL
- Enables sorting and indexing operations on UUID columns
- The comparison is byte-wise lexicographic, not based on UUID timestamp or version
- Used internally by the PostgreSQL query executor for UUID comparison operations