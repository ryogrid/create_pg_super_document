# FullTransactionIdRetreat

## Location
[src/include/access/transam.h:103-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L103-L127)

## Overview
Decrements a FullTransactionId while skipping over transaction IDs that would appear special when viewed as 32-bit XIDs, ensuring proper handling of transaction ID space navigation.

## Definition
```c
static inline void
FullTransactionIdRetreat(FullTransactionId *dest)
```

## Detailed Description
This function decrements a FullTransactionId by one, but with special handling to avoid landing on transaction IDs that would be considered special when interpreted as 32-bit transaction IDs. It first decrements the value, then checks if the result is still within the normal transaction ID range for 64-bit XIDs. If the decremented value results in a 32-bit XID that would be special (less than FirstNormalTransactionId), the function continues decrementing until it reaches a safe value.

The function handles two types of special transaction IDs:
1. True special XIDs for 64-bit transactions (which can't be reached during normal wraparound)
2. XIDs that would appear special only when viewed as 32-bit XIDs

## Parameters / Member Variables
- `dest`: Pointer to a FullTransactionId that will be decremented and modified in place

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdPrecedes
  - FirstNormalFullTransactionId
  - XidFromFullTransactionId
  - FirstNormalTransactionId
- Called from (representative examples):
  - StartupXLOG
  - ExpireAllKnownAssignedTransactionIds

## Notes and Other Information
- This is a static inline function for performance
- Modifies the input parameter in place rather than returning a new value
- Essential for proper transaction ID space navigation in recovery scenarios
- Handles the complexity of 32-bit vs 64-bit transaction ID special values
- Used primarily during WAL replay and transaction cleanup operations
- The function ensures that retreated transaction IDs remain valid and usable in the PostgreSQL transaction system