# CreateCommandTag

## Location
src/backend/tcop/utility.c: 2360 - 3246

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
  - AlterObjectTypeCommandTag (for ALTER operations involving object types)
  - Various CommandTag constants (CMDTAG_SELECT, CMDTAG_INSERT, etc.)
  - Statement-specific structures (TransactionStmt, DropStmt, etc.)
  - Object type enums (OBJECT_TABLE, OBJECT_FUNCTION, etc.)
- Called from (representative examples):
  - EventTriggerGetTag (src/backend/commands/event_trigger.c:625)
  - standard_ProcessUtility (src/backend/tcop/utility.c:575)
  - exec_simple_query (src/backend/tcop/postgres.c:1123)
  - _SPI_prepare_plan (src/backend/executor/spi.c:2262)

## Notes and Other Information
- The function is recursive for RawStmt nodes, calling itself on the contained statement
- Handles both raw and cooked (analyzed) statements uniformly for utility commands
- Provides special handling for SELECT statements with row-level locking (FOR UPDATE, FOR SHARE, etc.)
- For PlannedStmt and Query nodes, it examines the commandType field to determine the operation
- Returns CMDTAG_UNKNOWN for unrecognized node types or command types, ensuring safe fallback behavior
- Critical for PostgreSQL's command logging and monitoring infrastructure
- Used extensively in access control decisions and command completion reporting