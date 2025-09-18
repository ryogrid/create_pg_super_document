# BTPendingFSM

## Location
src/include/access/nbtree.h: 324 - 328

## Overview
BTPendingFSM is a structure used during VACUUM operations to track deleted B-tree pages that are pending addition to the Free Space Map (FSM) for future recycling.

## Definition


## Detailed Description
BTPendingFSM is part of PostgreSQL's B-tree VACUUM infrastructure, specifically designed to manage the lifecycle of deleted pages during vacuum operations. When VACUUM deletes B-tree pages, they cannot be immediately recycled because concurrent transactions might still need to access them.

This structure serves as a temporary holder for information about deleted pages that are candidates for being added to the Free Space Map (FSM). The FSM tracks available space that can be reused for new data. By maintaining the transaction ID associated with each deleted page, VACUUM can later determine when it's safe to mark these pages as available for reuse.

The structure is used internally by the B-tree vacuum process and is exported to nbtpage.c for use by page deletion related functions. It represents an intermediate state between page deletion and final recycling.

## Parameters / Member Variables
- : Block number of the page that was deleted by the current VACUUM operation
- : The full transaction ID from the deleted page's BTDeletedPageData.safexid field, used to determine when the page can be safely recycled

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId (transaction identifier type)
  - BlockNumber (for page references)
- Called from (representative examples):
  - _bt_pendingfsm_init (initialize pending FSM tracking)
  - _bt_pendingfsm_add (add a page to pending FSM list)
  - BTVacState (contains array of BTPendingFSM structures)

## Notes and Other Information
- This structure is private to nbtree.c but exported for use by page deletion code in nbtpage.c
- It represents an intermediate state in the page recycling pipeline during VACUUM operations
- The safexid field corresponds directly to the safexid stored in BTDeletedPageData when the page was deleted
- Pages tracked in BTPendingFSM structures will eventually be added to the FSM when their transaction IDs become old enough
- This mechanism ensures that page recycling follows MVCC rules and doesn't interfere with concurrent transactions
- The structure is part of a larger VACUUM state management system that coordinates safe page deletion and recycling
- Used in conjunction with BTVacState to manage the overall vacuum process