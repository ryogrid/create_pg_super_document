# xidneq

## Location
[src/backend/utils/adt/xid.c:92-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L92-L103)

## Overview
The  function is a PostgreSQL built-in function that compares two transaction IDs (XIDs) and returns true if they are different.

## Definition

```c
Datum
xidneq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the inequality comparison operator for transaction IDs in PostgreSQL. It takes two TransactionId parameters through the PostgreSQL function call interface and returns a boolean value indicating whether the two transaction IDs are different. The function uses the  macro internally and negates its result to determine inequality.

## Parameters / Member Variables
- : PostgreSQL function argument structure containing:
  - First argument:  - The first transaction ID to compare
  - Second argument:  - The second transaction ID to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TRANSACTIONID (macro for extracting TransactionId arguments)
  - TransactionIdEquals (macro for comparing transaction IDs)
  - PG_RETURN_BOOL (macro for returning boolean values)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of the xid data type operator family in PostgreSQL
- It provides the '<>' operator functionality for transaction IDs
- The function is registered in the system catalogs as a built-in operator function
- Located in src/backend/utils/adt/xid.c:92-103