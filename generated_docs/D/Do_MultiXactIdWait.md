# Do_MultiXactIdWait

## Location
[src/backend/access/heap/heapam.c:7673-7750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7673-L7750)

## Overview
Do_MultiXactIdWait is the core implementation function that waits for conflicting members of a multixact to complete, supporting both blocking and non-blocking wait modes.

## Definition

```c
static bool
Do_MultiXactIdWait(MultiXactId multi, MultiXactStatus status,
				   uint16 infomask, bool nowait,
				   Relation rel, ItemPointer ctid, XLTW_Oper oper,
				   int *remaining)
```
## Detailed Description
This function implements the actual waiting logic for multixact conflicts by:

1. Retrieving all members of the specified multixact ID
2. Iterating through each member to identify conflicts with the requested status
3. Skipping members that belong to the current backend (to avoid deadlock)
4. Using XactLockTableWait or ConditionalXactLockTableWait to wait for conflicting transactions
5. Tracking the number of remaining active members

The function handles pre-upgrade tuples as a special case and supports both conditional (nowait) and unconditional waiting modes. It ensures that by completion, all conflicting transactions from other backends have finished, though the caller may need to iterate if the tuple's Xmax has changed during the wait.

## Parameters / Member Variables
- `multi`: The multixact ID whose members need to be waited for
- `status`: The lock status being requested, used to determine conflicts
- `infomask`: Tuple header information mask for optimization
- `nowait`: If true, use conditional locking to avoid blocking
- `rel`: Relation for error context information
- `ctid`: Tuple identifier for error context
- `oper`: Operation type for error context
- `*remaining`: Output parameter for count of remaining active members (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - HEAP_LOCKED_UPGRADED
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - HEAP_XMAX_IS_LOCKED_ONLY
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [DoLockModesConflict](DoLockModesConflict.md)
  - LOCKMODE_from_mxstatus
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md)
  - [ConditionalXactLockTableWait](../C/ConditionalXactLockTableWait.md)
  - [XactLockTableWait](../X/XactLockTableWait.md)
- Called from (representative examples):
  - [MultiXactIdWait](../M/MultiXactIdWait.md)
  - [ConditionalMultiXactIdWait](../C/ConditionalMultiXactIdWait.md)

## Notes and Other Information
This is a static helper function that serves as the common implementation for both MultiXactIdWait and ConditionalMultiXactIdWait. It's critical for PostgreSQL's tuple locking mechanism, ensuring proper synchronization when multiple transactions compete for tuple access. The function carefully avoids waiting on transactions from the same backend to prevent assertion failures and deadlocks. The remaining count is unreliable when the function returns false (in nowait mode).

## Simplified Source

```c
static bool Do_MultiXactIdWait(MultiXactId multi, MultiXactStatus status,
                              uint16 infomask, bool nowait,
                              Relation rel, ItemPointer ctid, XLTW_Oper oper,
                              int *remaining)
{
    bool result = true;
    MultiXactMember *members;
    int nmembers;
    int remain = 0;

    // Handle pre-pg_upgrade tuples
    nmembers = HEAP_LOCKED_UPGRADED(infomask) ? -1 :
        GetMultiXactIdMembers(multi, &members, false,
                             HEAP_XMAX_IS_LOCKED_ONLY(infomask));

    if (nmembers >= 0) {
        for (int i = 0; i < nmembers; i++) {
            TransactionId memxid = members[i].xid;
            MultiXactStatus memstatus = members[i].status;

            // Skip our own transaction
            if (TransactionIdIsCurrentTransactionId(memxid)) {
                remain++;
                continue;
            }

            // Skip non-conflicting lock modes
            if (!DoLockModesConflict(LOCKMODE_from_mxstatus(memstatus),
                                   LOCKMODE_from_mxstatus(status))) {
                if (remaining && TransactionIdIsInProgress(memxid))
                    remain++;
                continue;
            }

            // Wait for conflicting transaction
            if (nowait) {
                result = ConditionalXactLockTableWait(memxid);
                if (!result)
                    break;
            } else {
                XactLockTableWait(memxid, rel, ctid, oper);
            }
        }
        pfree(members);
    }

    if (remaining)
        *remaining = remain;

    return result;
}
```

This function:
1. Gets all members of the MultiXact
2. Skips members from the current transaction
3. Checks for lock mode conflicts
4. Waits for conflicting transactions (conditionally or unconditionally)
5. Returns success/failure and count of remaining active members