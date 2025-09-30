# ClassifyUtilityCommandAsReadOnly

## Location
[src/backend/tcop/utility.c:128-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L128-L403)

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
  - [Command](Command.md) classification flags (COMMAND_IS_NOT_READ_ONLY, COMMAND_IS_STRICTLY_READ_ONLY, COMMAND_OK_IN_READ_ONLY_TXN, COMMAND_OK_IN_RECOVERY)
  - Statement-specific structures (CopyStmt, LockStmt, TransactionStmt)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (src/backend/tcop/utility.c:571)

## Notes and Other Information
- Returns COMMAND_IS_NOT_READ_ONLY for DDL commands and TRUNCATE
- ALTER SYSTEM is considered strictly read-only despite writing to postgresql.auto.conf
- COPY FROM is OK in read-only transactions when targeting temporary tables
- Lock statements with modes stronger than RowExclusiveLock are restricted during recovery
- Transaction control statements have nuanced classifications based on their specific type
- Used by the utility command processing framework to enforce appropriate execution restrictions

## Simplified Source

```c
static int ClassifyUtilityCommandAsReadOnly(Node *parsetree) {
    switch (nodeTag(parsetree)) {
        // DDL commands - not read-only
        case T_AlterTableStmt:
        case T_CreateStmt:
        case T_DropStmt:
        case T_IndexStmt:
        case T_TruncateStmt:
        // ... (many other DDL command types)
            return COMMAND_IS_NOT_READ_ONLY;

        // ALTER SYSTEM - special case, considered read-only
        case T_AlterSystemStmt:
            // Writes config file but doesn't affect WAL or pg_dump
            return COMMAND_IS_STRICTLY_READ_ONLY;

        // Commands that only affect backend-local state
        case T_DeallocateStmt:
        case T_DiscardStmt:
        case T_VariableSetStmt:
        // ... (other local state commands)
            // OK in read-only transactions but not parallel mode
            return COMMAND_OK_IN_RECOVERY | COMMAND_OK_IN_READ_ONLY_TXN;

        // Maintenance commands that write WAL
        case T_ClusterStmt:
        case T_ReindexStmt:
        case T_VacuumStmt:
            // Write WAL but don't change logical database state
            return COMMAND_OK_IN_READ_ONLY_TXN;

        // COPY command - depends on direction
        case T_CopyStmt:
            CopyStmt *stmt = (CopyStmt *) parsetree;
            if (stmt->is_from)
                return COMMAND_OK_IN_READ_ONLY_TXN;  // COPY FROM (to temp tables)
            else
                return COMMAND_IS_STRICTLY_READ_ONLY;  // COPY TO

        // Query and display commands
        case T_ExplainStmt:
        case T_VariableShowStmt:
            return COMMAND_IS_STRICTLY_READ_ONLY;

        // Lock commands - depends on lock strength
        case T_LockStmt:
            LockStmt *stmt = (LockStmt *) parsetree;
            if (stmt->mode > RowExclusiveLock)
                return COMMAND_OK_IN_READ_ONLY_TXN;
            else
                return COMMAND_IS_STRICTLY_READ_ONLY;

        // Transaction control - varies by type
        case T_TransactionStmt:
            TransactionStmt *stmt = (TransactionStmt *) parsetree;
            switch (stmt->kind) {
                case TRANS_STMT_BEGIN:
                case TRANS_STMT_COMMIT:
                case TRANS_STMT_ROLLBACK:
                case TRANS_STMT_SAVEPOINT:
                    return COMMAND_IS_STRICTLY_READ_ONLY;

                case TRANS_STMT_PREPARE:
                case TRANS_STMT_COMMIT_PREPARED:
                case TRANS_STMT_ROLLBACK_PREPARED:
                    return COMMAND_OK_IN_READ_ONLY_TXN;
            }
            break;

        default:
            elog(ERROR, "unrecognized node type: %d", nodeTag(parsetree));
    }

    return 0;
}
```