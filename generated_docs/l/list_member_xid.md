# list_member_xid

## Location
src/backend/nodes/list.c: 742 - 766

## Overview
Tests whether a given TransactionId (XID) value is a member of a transaction ID list using direct XID comparison for equality determination.

## Definition
```c
bool list_member_xid(const List *list, TransactionId datum)
```

## Detailed Description
The `list_member_xid` function performs membership testing on PostgreSQL's List data structure specifically for TransactionId lists. It iterates through the list cells using the `foreach` macro and compares each cell's XID value with the target datum using direct XID comparison (`==` operator). The function includes assertions to ensure the input list is actually an XID list type and validates list invariants for debugging purposes.

This function is optimized for TransactionId comparison and should only be used with lists that contain XID values. TransactionIds are PostgreSQL's internal identifiers for database transactions, used extensively in MVCC (Multi-Version Concurrency Control) and transaction management. The function uses the `lfirst_xid` macro to extract XID values from list cells.

## Parameters / Member Variables
- `list`: A constant pointer to the List structure to search within. Must be an XID list type.
- `datum`: A TransactionId value representing the target transaction identifier to search for in the list.

## Dependencies
- Functions called/Symbols referenced:
  - IsXidList - Validates that the list contains TransactionId values
  - check_list_invariants - Performs debugging validation of list structure
  - foreach - Macro for iterating through list cells
  - lfirst_xid - Macro for accessing the XID value of a list cell

- Called from (representative examples):
  - pa_start_subtrans - Used in parallel apply worker for subtransaction handling
  - get_schema_sent_in_streamed_txn - Used in logical replication for schema tracking in streamed transactions

## Notes and Other Information
- The function uses direct TransactionId value comparison (typically 32-bit unsigned integers)
- Only suitable for XID lists; will assert if used with other list types
- Returns `true` if the XID is found, `false` otherwise
- Part of PostgreSQL's generic List API located in src/backend/nodes/list.c
- Specialized for transaction management and logical replication scenarios
- Less commonly used compared to other list_member variants, primarily appearing in replication and transaction control contexts
- Type-safe alternative to generic list membership functions when working with PostgreSQL transaction identifiers
- Essential for tracking transaction states in parallel replication workers and logical decoding processes