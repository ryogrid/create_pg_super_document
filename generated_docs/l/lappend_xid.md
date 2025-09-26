# lappend_xid

## Location
[src/backend/nodes/list.c:393-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L393-L414)

## Overview
Appends a TransactionId (XID) value to a PostgreSQL XidList data structure, returning a pointer to the modified list.

## Definition

```c
List *
lappend_xid(List *list, TransactionId datum)
```
## Detailed Description
The  function is a specialized version of  designed specifically for transaction ID lists (T_XidList). It appends a TransactionId value to the end of an XidList, handling both empty lists (NIL) and existing lists with elements. Like other lappend variants, this function may or may not destructively modify the original list structure, so callers must use the returned value rather than the original list pointer.

When the input list is NIL, the function creates a new XidList with a single TransactionId element. For existing lists, it adds a new tail cell and stores the TransactionId value. The function includes type assertions to ensure the list is specifically a transaction ID list and performs invariant checking.

This function is used in PostgreSQL's transaction management and replication systems for maintaining collections of transaction identifiers.

## Parameters / Member Variables
- : The XidList to append to, or NIL to create a new transaction ID list
- : The TransactionId value to be appended to the list

## Dependencies
- Functions called/Symbols referenced:
  - IsXidList (assertion check for XID list type)
  - [new_list](../n/new_list.md) (creates new list when input is NIL, with T_XidList type)
  - [new_tail_cell](../n/new_tail_cell.md) (adds new cell to existing list)
  - llast_xid (macro to access last TransactionId element of list)
  - [check_list_invariants](../c/check_list_invariants.md) (debugging/validation function)
- Called from (representative examples):
  - [nodeRead](../n/nodeRead.md) (node reading/parsing operations)
  - [pa_start_subtrans](../p/pa_start_subtrans.md) (parallel apply subtransaction handling)
  - [set_schema_sent_in_streamed_txn](../s/set_schema_sent_in_streamed_txn.md) (logical replication schema tracking)
  - forfive (list iteration macro)

## Notes and Other Information
- Specialized for TransactionId values only, ensuring type safety in transaction management
- Primarily used in replication and transaction processing contexts
- Must use return value as function may reallocate the list structure
- Less commonly used compared to other lappend variants, reflecting its specialized role in transaction handling
- Critical for maintaining transaction state in logical replication and parallel processing