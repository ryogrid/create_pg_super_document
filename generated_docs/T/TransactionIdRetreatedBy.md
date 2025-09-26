# TransactionIdRetreatedBy

## Location
[src/include/access/transam.h:322-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L322-L333)

## Overview
Returns a transaction ID that is backed up by a specified amount, handling PostgreSQL's modular transaction ID wraparound correctly.

## Definition

```c
static inline TransactionId
TransactionIdRetreatedBy(TransactionId xid, uint32 amount)
```
## Detailed Description
This function calculates a transaction ID that is  positions before the given transaction ID  in PostgreSQL's circular transaction ID space. The function handles the special case where the subtraction might result in a transaction ID below , in which case it continues decrementing to maintain the proper wraparound behavior in the modular arithmetic system used by PostgreSQL for transaction IDs.

The function ensures that the returned transaction ID respects PostgreSQL's transaction ID numbering scheme, where transaction IDs wrap around in a circular fashion to handle the finite 32-bit transaction ID space.

## Parameters / Member Variables
- : The starting transaction ID from which to retreat
- : The number of positions to move backward in the transaction ID sequence

## Dependencies
- Functions called/Symbols referenced:
  - FirstNormalTransactionId
- Called from (representative examples):
  - No references found in the codebase

## Notes and Other Information
- This is an inline function defined in the transaction management header file
- The function handles wraparound by continuing to decrement when the result falls below FirstNormalTransactionId
- Currently appears to be unused in the codebase, possibly reserved for future use or specific edge cases
- The function complements other transaction ID comparison and manipulation utilities in PostgreSQL's transaction management system