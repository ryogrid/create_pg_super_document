# xid8ne

## Location
src/backend/utils/adt/xid.c: 232 - 240

## Overview
The xid8ne function is a PostgreSQL built-in function that compares two 8-byte transaction IDs (xid8) for inequality.

## Definition
```c
Datum xid8ne(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the inequality comparison operator for the xid8 data type in PostgreSQL. It takes two FullTransactionId values as input arguments and returns a boolean result indicating whether they represent different transaction IDs. The function is the complement of xid8eq, returning true when the transaction IDs are not equal and false when they are equal.

The function follows the standard PostgreSQL function calling convention, using PG_FUNCTION_ARGS macro to access its parameters and PG_RETURN_BOOL to return the boolean result. It internally uses FullTransactionIdEquals and negates the result to achieve the inequality comparison.

## Parameters / Member Variables
- `fxid1`: First FullTransactionId value obtained from function argument 0
- `fxid2`: Second FullTransactionId value obtained from function argument 1

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FULLTRANSACTIONID (macro to extract FullTransactionId from function arguments)
  - FullTransactionIdEquals (function to compare two FullTransactionId values)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of the xid8 data type operator family
- It's typically invoked through SQL inequality comparisons using the '<>' or '!=' operators on xid8 values
- The function is located in src/backend/utils/adt/xid.c along with other transaction ID utility functions
- Uses PostgreSQL's standard function interface macros for argument handling and return value management
- Implements inequality by negating the result of FullTransactionIdEquals