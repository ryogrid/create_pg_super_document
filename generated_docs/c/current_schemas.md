# current_schemas

## Location
[src/backend/utils/adt/name.c:294-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/name.c#L294-L332)

## Overview
The current_schemas function is a SQL function that returns an array of all schema names in the current search path.

## Definition


## Detailed Description
This function implements the SQL standard CURRENT_SCHEMAS function. It takes a boolean parameter indicating whether to include implicit schemas (like pg_catalog) in the result. The function retrieves the complete search path and converts all schema OIDs to their corresponding names, returning them as a PostgreSQL array.

The function iterates through each schema in the search path, converts valid schema OIDs to names, and builds an array of name datums. It handles deleted or invalid schemas by skipping them rather than including null entries.

## Parameters / Member Variables
- The function accepts one boolean argument via PG_GETARG_BOOL(0):
  - When true: includes implicit schemas in the result
  - When false: returns only explicitly set schemas

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_search_path](../f/fetch_search_path.md): Retrieves the current search path, with boolean parameter controlling inclusion of implicit schemas
  - PG_GETARG_BOOL: Macro to extract boolean argument from function call
  - list_length: Gets the length of the search path list
  - [palloc](../p/palloc.md): Allocates memory for the names array
  - lfirst_oid: Gets the OID from each list cell during iteration
  - [get_namespace_name](../g/get_namespace_name.md): Converts namespace OID to its name string
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to PostgreSQL Datum
  - DirectFunctionCall1: Directly calls a PostgreSQL function with one argument
  - namein: Input function for the name data type
  - [list_free](../l/list_free.md): Frees the search path list memory
  - [construct_array_builtin](construct_array_builtin.md): Creates a PostgreSQL array from the collected names
  - PG_RETURN_POINTER: Macro to return a pointer (array) from a PostgreSQL function

- Called from (representative examples):
  - This function appears to be called directly from SQL queries rather than from internal C code

## Notes and Other Information
- This function is part of the SQL standard and provides complete schema context information
- Returns all schemas in the search path as an array, unlike current_schema which returns only the first one
- The boolean parameter controls whether system schemas are included in the result
- The function gracefully handles deleted schemas by excluding them from the result array
- The function is defined in src/backend/utils/adt/name.c alongside other name-related functions
- Returns an array of PostgreSQL's 'name' data type elements
- Essential for understanding the complete schema resolution context