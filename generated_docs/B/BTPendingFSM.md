# BTPendingFSM

## Location
[src/include/access/nbtree.h:324-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L324-L328)

## Overview
BTPendingFSM is a structure used during VACUUM operations to track deleted B-tree pages that are pending addition to the Free Space Map (FSM) for future recycling.

## Definition

```c
typedef struct BTPendingFSM
{
	BlockNumber target;			/* Page deleted by current VACUUM */
	FullTransactionId safexid;	/* Page's BTDeletedPageData.safexid */
} BTPendingFSM;
```
## Detailed Description
BTPendingFSM is part of PostgreSQL's B-tree VACUUM infrastructure, specifically designed to manage the lifecycle of deleted pages during vacuum operations. When VACUUM deletes B-tree pages, they cannot be immediately recycled because concurrent transactions might still need to access them.

This structure serves as a temporary holder for information about deleted pages that are candidates for being added to the Free Space Map (FSM). The FSM tracks available space that can be reused for new data. By maintaining the transaction ID associated with each deleted page, VACUUM can later determine when it's safe to mark these pages as available for reuse.

The structure is used internally by the B-tree vacuum process and is exported to nbtpage.c for use by page deletion related functions. It represents an intermediate state between page deletion and final recycling.

## Parameters / Member Variables
- `target`: Block number of the page that was deleted by the current VACUUM operation
- `safexid`: The full transaction ID from the deleted page's BTDeletedPageData.safexid field, used to determine when the page can be safely recycled
## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionId](../F/FullTransactionId.md) (transaction identifier type)
  - BlockNumber (for page references)
- Called from (representative examples):
  - [_bt_pendingfsm_init](../b/_bt_pendingfsm_init.md) (initialize pending FSM tracking)
  - [_bt_pendingfsm_add](../b/_bt_pendingfsm_add.md) (add a page to pending FSM list)
  - [BTVacState](BTVacState.md) (contains array of BTPendingFSM structures)

## Notes and Other Information
- This structure is private to nbtree.c but exported for use by page deletion code in nbtpage.c
- It represents an intermediate state in the page recycling pipeline during VACUUM operations
- The safexid field corresponds directly to the safexid stored in BTDeletedPageData when the page was deleted
- Pages tracked in BTPendingFSM structures will eventually be added to the FSM when their transaction IDs become old enough
- This mechanism ensures that page recycling follows MVCC rules and doesn't interfere with concurrent transactions
- The structure is part of a larger VACUUM state management system that coordinates safe page deletion and recycling
- Used in conjunction with BTVacState to manage the overall vacuum process