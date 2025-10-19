# xid8ne

## Location
[src/backend/utils/adt/xid.c:232-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L232-L240)

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

## Simplified Source

```c
Datum xid8ne(PG_FUNCTION_ARGS) {
    // Extract the two 64-bit transaction IDs from function arguments
    FullTransactionId fxid1 = PG_GETARG_FULLTRANSACTIONID(0);
    FullTransactionId fxid2 = PG_GETARG_FULLTRANSACTIONID(1);

    // Return true if the transaction IDs are different
    PG_RETURN_BOOL(!FullTransactionIdEquals(fxid1, fxid2));
}
```