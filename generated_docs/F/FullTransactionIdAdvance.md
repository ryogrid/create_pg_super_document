# FullTransactionIdAdvance

## Location
[src/include/access/transam.h:128-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L128-L140)

## Overview
Increments a FullTransactionId while skipping over transaction IDs that would appear special when viewed as 32-bit XIDs, ensuring proper handling of transaction ID space navigation.

## Definition
```c
static inline void
FullTransactionIdAdvance(FullTransactionId *dest)
```

## Detailed Description
This function increments a FullTransactionId by one, but with special handling to avoid landing on transaction IDs that would be considered special when interpreted as 32-bit transaction IDs. It first increments the value, then checks if the result is still within the normal transaction ID range for 64-bit XIDs. If the incremented value results in a 32-bit XID that would be special (less than FirstNormalTransactionId), the function continues incrementing until it reaches a safe value.

This function is the counterpart to FullTransactionIdRetreat and handles the same two types of special transaction IDs:
1. True special XIDs for 64-bit transactions (which can't be reached during normal wraparound)
2. XIDs that would appear special only when viewed as 32-bit XIDs

## Parameters / Member Variables
- `dest`: Pointer to a FullTransactionId that will be incremented and modified in place

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdPrecedes
  - FirstNormalFullTransactionId
  - XidFromFullTransactionId
  - FirstNormalTransactionId
- Called from (representative examples):
  - [GetNewTransactionId](../G/GetNewTransactionId.md)
  - [GetSnapshotData](../G/GetSnapshotData.md)

## Notes and Other Information
- This is a static inline function for performance
- Modifies the input parameter in place rather than returning a new value
- Essential for proper transaction ID allocation and snapshot management
- Handles the complexity of 32-bit vs 64-bit transaction ID special values
- Used primarily during transaction ID assignment and snapshot creation
- The function ensures that advanced transaction IDs remain valid and usable in the PostgreSQL transaction system
- Comment references FullTransactionIdAdvance() itself, indicating shared logic with retreat function