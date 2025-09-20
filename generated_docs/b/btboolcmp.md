# btboolcmp

## Location
[src/backend/access/nbtree/nbtcompare.c:73-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtcompare.c#L73-L81)

## Overview
This function provides a comparison function for boolean values in PostgreSQL's B-tree index operations, returning the standard comparison result (-1, 0, 1) for ordering boolean values.

## Definition

```c
Datum
btboolcmp(PG_FUNCTION_ARGS)
```
## Detailed Description
btboolcmp is a PostgreSQL built-in function that implements comparison logic for boolean data types within B-tree indexes. The function follows PostgreSQL's standard comparison function interface, taking two boolean arguments and returning an integer that indicates their relative ordering. The comparison is implemented by casting the boolean values to integers and subtracting them, which naturally produces the correct comparison result since false (0) < true (1) in PostgreSQL's boolean ordering.

## Parameters / Member Variables
- : PostgreSQL macro that expands to accept function arguments through the standard fmgr interface
  - First argument (index 0): Boolean value 'a' retrieved via PG_GETARG_BOOL(0)
  - Second argument (index 1): Boolean value 'b' retrieved via PG_GETARG_BOOL(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL: Macro to extract boolean arguments from the function call context
  - PG_RETURN_INT32: Macro to return a 32-bit integer result
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- The function implements the standard PostgreSQL comparison semantics where false < true
- The arithmetic subtraction (int32) a - (int32) b naturally produces -1 for false < true, 0 for equal values, and 1 for true > false
- This is a core function for B-tree indexing operations on boolean columns
- Located in src/backend/access/nbtree/nbtcompare.c, which contains comparison functions for various PostgreSQL data types