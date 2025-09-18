# MultiXactIdCreate

## Location
[src/backend/access/transam/multixact.c:433-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L433-L485)

## Overview
Creates a new MultiXactId representing exactly two TransactionIds with their respective statuses, providing a fundamental building block for PostgreSQL's multi-transaction locking system.

## Definition
```c
MultiXactId MultiXactIdCreate(TransactionId xid1, MultiXactStatus status1,
                             TransactionId xid2, MultiXactStatus status2)
```

## Detailed Description
This function constructs a new MultiXactId that represents two specific transactions, each with their own locking status. MultiXactIds are used in PostgreSQL to handle cases where multiple transactions need to hold different types of locks on the same tuple simultaneously. This is a specialized version for the common case of exactly two transactions.

The function performs several validation checks:
- Ensures both transaction IDs are valid
- Verifies that either the XIDs are different OR they have different statuses
- Confirms that MultiXactIdSetOldestMember() has been called to initialize the oldest member tracking

The function creates a MultiXactMember array with the two provided transactions and their statuses, then delegates to MultiXactIdCreateFromMembers() to perform the actual MultiXactId creation and storage.

## Parameters / Member Variables
- `xid1`: First TransactionId to include in the MultiXact
- `status1`: MultiXactStatus indicating the lock type/status for xid1
- `xid2`: Second TransactionId to include in the MultiXact  
- `status2`: MultiXactStatus indicating the lock type/status for xid2

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid (validation function)
  - TransactionIdEquals (comparison function)
  - MultiXactIdIsValid (validation function)
  - [MultiXactIdCreateFromMembers](MultiXactIdCreateFromMembers.md) (core creation function)
  - [MultiXactMember](MultiXactMember.md) (structure type)
  - [MultiXactStatus](MultiXactStatus.md) (enum type)
  - debug_elog3, mxid_to_string (debugging functions)
- Called from (representative examples):
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md) (heap access management)

## Notes and Other Information
- This function is optimized for the common case of exactly two transactions, avoiding the overhead of variable-length arrays
- Unlike MultiXactIdExpand, this function does not verify that both XIDs are still running, assuming the caller has performed necessary checks
- The function includes debug logging to trace MultiXact creation when debugging is enabled
- This is part of PostgreSQL's tuple-level locking mechanism that allows multiple transactions to hold compatible locks simultaneously
- The function assumes that MultiXactIdSetOldestMember() has been called to properly initialize the oldest member tracking system
- The resulting MultiXactId can be stored in tuple headers to represent complex locking scenarios