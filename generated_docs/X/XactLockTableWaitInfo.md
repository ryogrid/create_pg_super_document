# XactLockTableWaitInfo

## Location
src/backend/storage/lmgr/lmgr.c: 55 - 60

## Overview
XactLockTableWaitInfo is a struct that holds context information for transaction lock waits, providing details about the operation being performed and the tuple location being waited for.

## Definition


## Detailed Description
XactLockTableWaitInfo serves as a context structure for transaction lock waiting operations in PostgreSQL's lock manager. It encapsulates the necessary information to provide detailed error context when a transaction needs to wait for another transaction to complete. The struct is primarily used with the XactLockTableWait function to enable verbose error reporting during lock waits.

When a transaction needs to wait for another transaction (for example, when trying to update a tuple that's being modified by another transaction), this struct holds the contextual information that allows PostgreSQL to provide meaningful error messages to users about what operation is being blocked and which specific tuple is involved.

## Parameters / Member Variables
- : The type of operation that needs to wait (XLTW_Oper enum value), indicating what kind of operation is being blocked
- : The relation (table) containing the tuple being waited for
- : ItemPointer to the specific tuple (identified by its TID - tuple identifier) that is being waited for

## Dependencies
- Functions called/Symbols referenced:
  - XLTW_Oper (enum type for operation specification)
  - Relation (relation/table reference type)
  - ItemPointer (tuple identifier type)

- Called from (representative examples):
  - XactLockTableWait (uses this struct to set up error context callbacks)
  - XactLockTableWaitErrorCb (accesses this struct for error reporting)

## Notes and Other Information
- This struct is specifically designed for error context handling during transaction lock waits
- It is used in conjunction with PostgreSQL's error context callback mechanism to provide detailed information when lock waits occur
- The struct is typically stack-allocated and passed to error callback functions
- All three members are essential for providing complete context about the blocked operation
- The struct is defined in src/backend/storage/lmgr/lmgr.c at lines 55-60