# CreateCommandTag

## Location
[src/backend/tcop/utility.c:2360-3246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L2360-L3246)

## Overview
CreateCommandTag is a comprehensive utility function that determines the appropriate CommandTag for any PostgreSQL command operation, whether it's from a raw parse tree, analyzed Query, or PlannedStmt.

## Definition
```c
CommandTag CreateCommandTag(Node *parsetree)
```

## Detailed Description
This function serves as the central command classification system in PostgreSQL's utility command processing. It takes a Node pointer (which can represent various types of parse trees) and returns the corresponding CommandTag that identifies what type of SQL operation is being performed. The function handles all command types in PostgreSQL, from basic DML operations (SELECT, INSERT, UPDATE, DELETE, MERGE) to complex DDL operations and utility commands.

The function uses a large switch statement based on the node type (nodeTag) to categorize commands. It can process raw statements, planned statements, and parsed queries, handling recursive cases where necessary. For utility statements, it provides detailed sub-classification based on statement-specific properties (e.g., transaction type, object type, etc.).

This is a critical function for logging, auditing, command completion tracking, and access control decisions throughout PostgreSQL's command execution pipeline.

## Parameters / Member Variables
- `parsetree`: A Node pointer that can represent different types of parse trees including RawStmt, PlannedStmt, Query, or various statement-specific nodes

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine the type of the input node)
  - [AlterObjectTypeCommandTag](../A/AlterObjectTypeCommandTag.md) (for ALTER operations involving object types)
  - Various CommandTag constants (CMDTAG_SELECT, CMDTAG_INSERT, etc.)
  - Statement-specific structures (TransactionStmt, DropStmt, etc.)
  - Object type enums (OBJECT_TABLE, OBJECT_FUNCTION, etc.)
- Called from (representative examples):
  - [EventTriggerGetTag](../E/EventTriggerGetTag.md) (src/backend/commands/event_trigger.c:625)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (src/backend/tcop/utility.c:575)
  - [exec_simple_query](../e/exec_simple_query.md) (src/backend/tcop/postgres.c:1123)
  - [_SPI_prepare_plan](../S/_SPI_prepare_plan.md) (src/backend/executor/spi.c:2262)

## Notes and Other Information
- The function is recursive for RawStmt nodes, calling itself on the contained statement
- Handles both raw and cooked (analyzed) statements uniformly for utility commands
- Provides special handling for SELECT statements with row-level locking (FOR UPDATE, FOR SHARE, etc.)
- For PlannedStmt and Query nodes, it examines the commandType field to determine the operation
- Returns CMDTAG_UNKNOWN for unrecognized node types or command types, ensuring safe fallback behavior
- Critical for PostgreSQL's command logging and monitoring infrastructure
- Used extensively in access control decisions and command completion reporting

## Simplified Source

```c
// Simplified version of CreateCommandTag
CommandTag CreateCommandTag(Node *parsetree) {
    CommandTag tag;

    switch (nodeTag(parsetree)) {
        // Recursive case: unwrap raw statements
        case T_RawStmt:
            tag = CreateCommandTag(((RawStmt *) parsetree)->stmt);
            break;

        // Basic DML operations
        case T_InsertStmt:
            tag = CMDTAG_INSERT;
            break;
        case T_DeleteStmt:
            tag = CMDTAG_DELETE;
            break;
        case T_UpdateStmt:
            tag = CMDTAG_UPDATE;
            break;
        case T_MergeStmt:
            tag = CMDTAG_MERGE;
            break;
        case T_SelectStmt:
            tag = CMDTAG_SELECT;
            break;

        // Transaction control commands
        case T_TransactionStmt:
            {
                TransactionStmt *stmt = (TransactionStmt *) parsetree;
                switch (stmt->kind) {
                    case TRANS_STMT_BEGIN:
                        tag = CMDTAG_BEGIN;
                        break;
                    case TRANS_STMT_COMMIT:
                        tag = CMDTAG_COMMIT;
                        break;
                    case TRANS_STMT_ROLLBACK:
                        tag = CMDTAG_ROLLBACK;
                        break;
                    // ... other transaction types
                    default:
                        tag = CMDTAG_UNKNOWN;
                }
            }
            break;

        // DDL operations - CREATE commands
        case T_CreateStmt:
            tag = CMDTAG_CREATE_TABLE;
            break;
        case T_CreateFunctionStmt:
            if (((CreateFunctionStmt *) parsetree)->is_procedure)
                tag = CMDTAG_CREATE_PROCEDURE;
            else
                tag = CMDTAG_CREATE_FUNCTION;
            break;
        case T_IndexStmt:
            tag = CMDTAG_CREATE_INDEX;
            break;

        // DDL operations - DROP commands
        case T_DropStmt:
            switch (((DropStmt *) parsetree)->removeType) {
                case OBJECT_TABLE:
                    tag = CMDTAG_DROP_TABLE;
                    break;
                case OBJECT_FUNCTION:
                    tag = CMDTAG_DROP_FUNCTION;
                    break;
                case OBJECT_INDEX:
                    tag = CMDTAG_DROP_INDEX;
                    break;
                // ... many other object types
                default:
                    tag = CMDTAG_UNKNOWN;
            }
            break;

        // DDL operations - ALTER commands
        case T_AlterTableStmt:
            tag = AlterObjectTypeCommandTag(((AlterTableStmt *) parsetree)->objtype);
            break;

        // Utility commands
        case T_VacuumStmt:
            if (((VacuumStmt *) parsetree)->is_vacuumcmd)
                tag = CMDTAG_VACUUM;
            else
                tag = CMDTAG_ANALYZE;
            break;
        case T_ExplainStmt:
            tag = CMDTAG_EXPLAIN;
            break;
        case T_CopyStmt:
            tag = CMDTAG_COPY;
            break;

        // Already-planned queries
        case T_PlannedStmt:
            {
                PlannedStmt *stmt = (PlannedStmt *) parsetree;
                switch (stmt->commandType) {
                    case CMD_SELECT:
                        // Check for row-level locking
                        if (stmt->rowMarks != NIL) {
                            // Determine specific SELECT variant based on locking strength
                            tag = determine_select_for_tag(stmt->rowMarks);
                        } else {
                            tag = CMDTAG_SELECT;
                        }
                        break;
                    case CMD_UPDATE:
                        tag = CMDTAG_UPDATE;
                        break;
                    case CMD_INSERT:
                        tag = CMDTAG_INSERT;
                        break;
                    case CMD_DELETE:
                        tag = CMDTAG_DELETE;
                        break;
                    case CMD_UTILITY:
                        tag = CreateCommandTag(stmt->utilityStmt);
                        break;
                    default:
                        tag = CMDTAG_UNKNOWN;
                }
            }
            break;

        // Parsed queries
        case T_Query:
            {
                Query *stmt = (Query *) parsetree;
                switch (stmt->commandType) {
                    case CMD_SELECT:
                        // Similar row-locking logic as PlannedStmt
                        if (stmt->rowMarks != NIL) {
                            tag = determine_query_select_for_tag(stmt->rowMarks);
                        } else {
                            tag = CMDTAG_SELECT;
                        }
                        break;
                    case CMD_UPDATE:
                        tag = CMDTAG_UPDATE;
                        break;
                    case CMD_INSERT:
                        tag = CMDTAG_INSERT;
                        break;
                    case CMD_DELETE:
                        tag = CMDTAG_DELETE;
                        break;
                    case CMD_UTILITY:
                        tag = CreateCommandTag(stmt->utilityStmt);
                        break;
                    default:
                        tag = CMDTAG_UNKNOWN;
                }
            }
            break;

        // ... Many more statement types (DefineStmt, GrantStmt, etc.)
        // Each follows similar pattern of extracting relevant properties
        // and mapping to appropriate CommandTag

        default:
            // Unrecognized node type
            tag = CMDTAG_UNKNOWN;
            break;
    }

    return tag;
}
```

Key simplifications made:
- Consolidated 100+ case statements into representative examples showing the main patterns
- Abstracted repetitive switch blocks (like the massive DropStmt cases) into comments
- Simplified row-locking logic for SELECT statements into helper function calls
- Focused on the core algorithm: examine node type, extract relevant properties, map to CommandTag
- Maintained the essential recursive structure for RawStmt, PlannedStmt, and Query utilities
- Preserved the critical error handling pattern (CMDTAG_UNKNOWN for unrecognized types)
- Kept the most common command types to show the variety handled