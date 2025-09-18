# ExecReScanModifyTable

## Location
[src/backend/executor/nodeModifyTable.c:4961-4968](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L4961-L4968)

## Overview
A placeholder function that explicitly prevents rescanning of ModifyTable nodes by raising an error, as rescan semantics are not defined for DML operations.

## Definition


## Detailed Description
This function is part of PostgreSQL's executor node interface that requires all plan node types to implement a rescan method. However, for ModifyTable nodes, the concept of rescanning is semantically problematic and not currently supported.

The function immediately raises an ERROR with the message "ExecReScanModifyTable is not implemented". This is intentional because:
1. DML operations (INSERT, UPDATE, DELETE, MERGE) have side effects that cannot be safely repeated
2. Re-executing a ModifyTable operation could lead to duplicate modifications or inconsistent state
3. The semantics of what "rescanning" a modification operation should mean are unclear and potentially dangerous

## Parameters / Member Variables
- : ModifyTableState structure (parameter present for interface compliance but not used)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
- Data structures used:
  - [ModifyTableState](../M/ModifyTableState.md) (parameter only)
- Called from:
  - [ExecReScan](ExecReScan.md)

## Notes and Other Information
- This is a deliberate non-implementation rather than an oversight
- Part of the standard executor node interface requirements
- The error serves as a safety mechanism to prevent accidental attempts to rescan modification operations
- Unlike query nodes (SELECT operations) which can be safely rescanned, modification operations have permanent effects on data
- If rescan functionality were ever needed for ModifyTable nodes, it would require careful consideration of transaction semantics and side effects
- The function exists primarily to satisfy the executor's function pointer interface requirements