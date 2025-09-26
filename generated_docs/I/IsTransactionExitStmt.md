# IsTransactionExitStmt

## Location
[src/backend/tcop/postgres.c:2830-2846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2830-L2846)

## Overview
Determines whether a given parse tree represents a transaction exit statement that is allowed to execute during an aborted transaction state.

## Definition

```c
static bool
IsTransactionExitStmt(Node *parsetree)
```
## Detailed Description
This function checks if a parse tree node represents one of the transaction exit statements that PostgreSQL allows to execute even when a transaction is in an aborted state. These statements include COMMIT, PREPARE, ROLLBACK, and ROLLBACK TO SAVEPOINT. The function first verifies that the node is a TransactionStmt, then checks the specific transaction statement kind against the allowed types. This is crucial for PostgreSQL's error recovery mechanism, as it allows clients to properly exit failed transactions.

## Parameters / Member Variables
- `parsetree`: A Node pointer representing the parsed SQL statement to check

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionStmt](../T/TransactionStmt.md) (struct type)
  - TRANS_STMT_COMMIT
  - TRANS_STMT_PREPARE
  - TRANS_STMT_ROLLBACK
  - TRANS_STMT_ROLLBACK_TO
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_parse_message](../e/exec_parse_message.md)
  - [exec_bind_message](../e/exec_bind_message.md)
  - [IsTransactionExitStmtList](IsTransactionExitStmtList.md)

## Notes and Other Information
- Returns true only for transaction statements that can exit an aborted transaction
- Essential for PostgreSQL's transaction error handling and recovery
- Allows clients to issue cleanup commands even when transactions have failed
- Part of a family of functions for checking statement types in aborted transaction contexts
- Uses PostgreSQL's node type checking macros (IsA) for type safety