# xid8eq

## Location
[src/backend/utils/adt/xid.c:223-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L223-L231)

## Overview
The xid8eq function is a PostgreSQL built-in function that compares two 8-byte transaction IDs (xid8) for equality.

## Definition

```c
Datum
xid8eq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the equality comparison operator for the xid8 data type in PostgreSQL. It takes two FullTransactionId values as input arguments and returns a boolean result indicating whether they represent the same transaction ID. The function is part of PostgreSQL's transaction ID management system and is used internally by the database engine when comparing 8-byte transaction identifiers.

The function follows the standard PostgreSQL function calling convention, using PG_FUNCTION_ARGS macro to access its parameters and PG_RETURN_BOOL to return the boolean result.

## Parameters / Member Variables
- : First FullTransactionId value obtained from function argument 0
- : Second FullTransactionId value obtained from function argument 1

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FULLTRANSACTIONID (macro to extract FullTransactionId from function arguments)
  - FullTransactionIdEquals (function to compare two FullTransactionId values)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of the xid8 data type operator family
- It's typically invoked through SQL equality comparisons using the '=' operator on xid8 values
- The function is located in src/backend/utils/adt/xid.c along with other transaction ID utility functions
- Uses PostgreSQL's standard function interface macros for argument handling and return value management