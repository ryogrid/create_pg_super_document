# CommandIsReadOnly

## Location
[src/backend/tcop/utility.c:94-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L94-L127)

## Overview
CommandIsReadOnly determines whether an executable query is truly read-only, applying a stricter test than XactReadOnly mode to decide if CommandCounterIncrement should be skipped.

## Definition
`bool CommandIsReadOnly(PlannedStmt *pstmt)`

## Detailed Description
This function performs a strict read-only check on planned statements to determine if they can safely skip CommandCounterIncrement operations. Unlike the more permissive XactReadOnly mode checks, CommandIsReadOnly requires that the query be "in truth" read-only, meaning it performs no modifications whatsoever to the database state.

The function examines the command type and specific characteristics of SELECT statements to make this determination. For SELECT commands, it considers row-level locking (FOR UPDATE/SHARE) and data-modifying CTEs as disqualifying factors. All utility commands are conservatively treated as read/write operations.

## Parameters / Member Variables
- `pstmt`: PlannedStmt pointer representing the planned statement to evaluate for read-only status

## Dependencies
- Functions called/Symbols referenced:
  - [PlannedStmt](../P/PlannedStmt.md) (structure type)
  - CMD_SELECT, CMD_UPDATE, CMD_INSERT, CMD_DELETE, CMD_MERGE, CMD_UTILITY (command type constants)
- Called from (representative examples):
  - [init_execution_state](../i/init_execution_state.md) (src/backend/executor/functions.c:523)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md) (src/backend/executor/spi.c:1740)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md) (src/backend/executor/spi.c:2653)

## Notes and Other Information
- Returns false for SELECT statements with rowMarks (FOR UPDATE/SHARE clauses) or hasModifyingCTE flag set
- Conservatively returns false for all utility commands, as they require separate analysis
- Used primarily in contexts where avoiding CommandCounterIncrement is beneficial for performance
- Part of the query execution optimization framework in PostgreSQL