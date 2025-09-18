# IsTransactionStmtList

## Location
src/backend/tcop/postgres.c: 2862 - 2876

## Overview
Tests whether a list of PlannedStmt nodes contains a single TransactionStmt (BEGIN, COMMIT, ROLLBACK, SAVEPOINT, etc.).

## Definition
```c
static bool IsTransactionStmtList(List *pstmts)
```

## Detailed Description
This function is a utility helper that examines a list of planned statements to determine if it contains exactly one statement that is a TransactionStmt node. Unlike IsTransactionExitStmtList which specifically checks for transaction-ending statements, this function identifies any type of transaction control statement including BEGIN, COMMIT, ROLLBACK, SAVEPOINT, and RELEASE SAVEPOINT commands.

The function performs validation similar to IsTransactionExitStmtList:
1. Ensures the list contains exactly one statement
2. Verifies that the single statement is a utility command
3. Checks that the utility statement is specifically a TransactionStmt node type

This is used in PostgreSQL's query execution pipeline to identify transaction control statements that require special handling during execution.\n\n## Parameters / Member Variables\n- `pstmts`: A List pointer containing PlannedStmt nodes to be examined\n\n## Dependencies\n- Functions called/Symbols referenced:\n  - list_length (to check list size)\n  - linitial_node (to extract first PlannedStmt from list)\n  - PlannedStmt (structure type for planned statements)\n  - CMD_UTILITY (command type constant)\n  - IsA (macro to check node type)\n  - TransactionStmt (transaction statement node type)\n- Called from (representative examples):\n  - exec_execute_message (in src/backend/tcop/postgres.c:2145)\n\n## Notes and Other Information\n- This is a static function within postgres.c, making it internal to the query execution module\n- Returns true only for single-statement lists containing TransactionStmt nodes\n- Broader in scope than IsTransactionExitStmtList, covering all transaction control statements\n- Part of PostgreSQL's statement classification system for execution optimization\n- Used to identify statements that affect transaction state and require special processing