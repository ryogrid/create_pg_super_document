# ClassifyUtilityCommandAsReadOnly

## Location
src/backend/tcop/utility.c: 128 - 403

## Overview
ClassifyUtilityCommandAsReadOnly determines the degree to which a utility command is read-only by analyzing the command type and returning appropriate flags indicating execution restrictions.

## Definition
`static int ClassifyUtilityCommandAsReadOnly(Node *parsetree)`

## Detailed Description
This function performs a comprehensive classification of utility commands to determine their read-only characteristics and execution constraints. It returns a combination of flags that indicate whether a command can be executed in read-only transactions, during recovery, or in parallel mode.

The function uses a large switch statement to categorize different types of utility statements (DDL, administrative commands, transaction control, etc.) and assigns appropriate restriction flags. The classification considers factors such as WAL writing, database state modification, backend-local vs. global state changes, and compatibility with parallel execution.

Key classification categories include:
- Strictly read-only commands (safe in all contexts)
- Commands OK in read-only transactions but not parallel mode
- Commands that write WAL but don't affect pg_dump output
- DDL and modification commands that are not read-only

## Parameters / Member Variables
- `parsetree`: Node pointer representing the parsed utility statement to classify

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (for type identification)
  - Various command type constants (T_AlterTableStmt, T_CreateStmt, etc.)
  - Command classification flags (COMMAND_IS_NOT_READ_ONLY, COMMAND_IS_STRICTLY_READ_ONLY, COMMAND_OK_IN_READ_ONLY_TXN, COMMAND_OK_IN_RECOVERY)
  - Statement-specific structures (CopyStmt, LockStmt, TransactionStmt)
- Called from (representative examples):
  - standard_ProcessUtility (src/backend/tcop/utility.c:571)

## Notes and Other Information
- Returns COMMAND_IS_NOT_READ_ONLY for DDL commands and TRUNCATE
- ALTER SYSTEM is considered strictly read-only despite writing to postgresql.auto.conf
- COPY FROM is OK in read-only transactions when targeting temporary tables
- Lock statements with modes stronger than RowExclusiveLock are restricted during recovery
- Transaction control statements have nuanced classifications based on their specific type
- Used by the utility command processing framework to enforce appropriate execution restrictions