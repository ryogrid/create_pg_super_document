# xid8lt

## Location
src/backend/utils/adt/xid.c: 241 - 249

## Overview
The xid8lt function is a PostgreSQL built-in function that compares two 8-byte transaction IDs (xid8) to determine if the first is less than the second.

## Definition
```c
Datum xid8lt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than comparison operator for the xid8 data type in PostgreSQL. It takes two FullTransactionId values as input arguments and returns a boolean result indicating whether the first transaction ID precedes the second in the transaction ordering. The function uses FullTransactionIdPrecedes to perform the actual comparison, which accounts for the circular nature of transaction ID space and potential wraparound scenarios.

Transaction IDs in PostgreSQL have a limited range and can wrap around, so this function ensures proper ordering comparison even across wraparound boundaries. The function follows the standard PostgreSQL function calling convention.

## Parameters / Member Variables
- `fxid1`: First FullTransactionId value obtained from function argument 0
- `fxid2`: Second FullTransactionId value obtained from function argument 1

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FULLTRANSACTIONID (macro to extract FullTransactionId from function arguments)
  - FullTransactionIdPrecedes (function to determine if one FullTransactionId precedes another)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of the xid8 data type operator family
- It's typically invoked through SQL less-than comparisons using the '<' operator on xid8 values
- The function is located in src/backend/utils/adt/xid.c along with other transaction ID utility functions
- Uses FullTransactionIdPrecedes which handles transaction ID wraparound correctly
- Essential for ordering operations and range queries involving transaction IDs
- Uses PostgreSQL's standard function interface macros for argument handling and return value management