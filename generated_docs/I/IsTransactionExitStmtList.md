# IsTransactionExitStmtList

## Location
src/backend/tcop/postgres.c: 2847 - 2861

## Overview
Tests whether a list of PlannedStmt nodes contains a single transaction exit statement (COMMIT, ROLLBACK, etc.).

## Definition
```c
static bool IsTransactionExitStmtList(List *pstmts)
```

## Detailed Description
This function is a utility helper that examines a list of planned statements to determine if it contains exactly one statement that is a transaction exit command. It specifically checks for utility commands that terminate transactions, such as COMMIT, ROLLBACK, or other transaction-ending statements. The function is used in the PostgreSQL query execution pipeline to identify when a statement list represents a transaction termination operation.

The function performs a two-step validation:
1. Ensures the list contains exactly one statement
2. Verifies that the single statement is a utility command of transaction exit type

## Parameters / Member Variables
- `pstmts`: A List pointer containing PlannedStmt nodes to be examined

## Dependencies
- Functions called/Symbols referenced:
  - list_length (to check list size)
  - linitial_node (to extract first PlannedStmt from list)
  - PlannedStmt (structure type for planned statements)
  - CMD_UTILITY (command type constant)
  - IsTransactionExitStmt (to check if utility statement is transaction exit)
- Called from (representative examples):
  - exec_execute_message (in src/backend/tcop/postgres.c:2231)

## Notes and Other Information
- This is a static function within postgres.c, making it internal to the query execution module
- Returns true only for single-statement lists containing transaction exit commands
- Part of PostgreSQL's query execution flow where transaction boundaries need special handling\n- Used to optimize execution paths for transaction control statements