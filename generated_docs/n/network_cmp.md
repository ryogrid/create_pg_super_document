# network_cmp

## Location
[src/backend/utils/adt/network.c:425-436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L425-L436)

## Overview
PostgreSQL built-in function wrapper that provides comparison functionality for network address data types, accessible from SQL.

## Definition


## Detailed Description
This function serves as the PostgreSQL built-in function interface for comparing network addresses (inet and cidr types). It acts as a thin wrapper around , handling the PostgreSQL function calling conventions by extracting arguments from the function call context and returning results in the expected Datum format.

The function extracts two network address arguments using PostgreSQL's argument access macros and delegates the actual comparison logic to . This separation allows the core comparison logic to be reused by other internal functions while maintaining a clean interface for SQL-callable functions.

This function is typically used internally by PostgreSQL's query processing system for operations like ORDER BY clauses, comparison operators, and index operations on network data types.

## Parameters / Member Variables
- Uses  - PostgreSQL standard function argument structure containing:
  - First argument (index 0):  network address to compare  
  - Second argument (index 1):  network address to compare

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL argument extraction macro for inet types)
  -  (core comparison logic)
  -  (PostgreSQL return value macro)

- Called from (representative examples):
  - No direct references found (likely registered as a PostgreSQL built-in function and called through the function manager)

## Notes and Other Information
- Returns a Datum containing an int32 value: negative if first argument is less, zero if equal, positive if greater
- Part of PostgreSQL's built-in function system for network data types
- The function follows PostgreSQL's standard built-in function signature pattern using 
- Registered in PostgreSQL's system catalogs for use in SQL queries
- Essential for sorting, indexing, and comparison operations on inet/cidr columns in SQL
- Located in 