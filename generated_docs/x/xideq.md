# xideq

## Location
[src/backend/utils/adt/xid.c:80-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L80-L91)

## Overview
The xideq function implements the equality comparison operator for the xid (TransactionId) data type, determining whether two transaction IDs are equal.

## Definition
```c
Datum xideq(PG_FUNCTION_ARGS)
```

## Detailed Description
xideq serves as the equality operator function for the xid data type in PostgreSQL's operator system. It takes two TransactionId values and compares them for equality using the TransactionIdEquals() utility function. This function is used by PostgreSQL's query executor when evaluating equality expressions involving xid values, such as in WHERE clauses, joins, and other comparison operations.

## Parameters / Member Variables
- `xid1`: The first TransactionId value to compare
- `xid2`: The second TransactionId value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TRANSACTIONID (used twice for both parameters)
  - TransactionIdEquals
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator system infrastructure)

## Notes and Other Information
- This function is part of PostgreSQL's operator system for the xid data type
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Implements the '=' operator for xid comparisons in SQL expressions
- Delegates the actual comparison logic to TransactionIdEquals() for consistency
- Returns a boolean value indicating whether the two transaction IDs are equal
- Essential for query processing involving transaction ID comparisons

## Simplified Source

```c
Datum
xideq(PG_FUNCTION_ARGS)
{
    TransactionId xid1 = PG_GETARG_TRANSACTIONID(0);
    TransactionId xid2 = PG_GETARG_TRANSACTIONID(1);

    // Compare the two transaction IDs for equality
    PG_RETURN_BOOL(TransactionIdEquals(xid1, xid2));
}
```