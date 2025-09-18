# transformMergeStmt

## Location
src/backend/parser/parse_merge.c: 107 - 414

## Overview
The main function that transforms a parsed MERGE statement AST into a Query tree structure, handling all aspects of MERGE statement analysis including permissions, namespace management, and action transformation.

## Definition
```c
Query *transformMergeStmt(ParseState *pstate, MergeStmt *stmt)
```

## Detailed Description
This function is the primary entry point for transforming MERGE statements during the parsing phase. It performs comprehensive analysis and transformation including:

1. **Permissions Analysis**: Collects required permissions (INSERT, UPDATE, DELETE, SELECT) based on the action types in WHEN clauses
2. **Validation**: Checks for unreachable WHEN clauses (those specified after unconditional ones) and validates relation types
3. **Namespace Setup**: Establishes proper visibility for target and source relations, handling namespace conflicts
4. **Join Condition Processing**: Transforms the ON condition that defines how source and target relations are matched
5. **Action Transformation**: Processes each WHEN clause, transforming their conditions and target lists according to their match type and command type
6. **Query Structure Creation**: Builds the complete Query structure with proper RTEs, join trees, and action lists

The function handles the three types of MERGE actions:
- **MATCHED**: Actions for rows that exist in both source and target
- **NOT MATCHED BY TARGET**: Actions for source rows with no target match (typically INSERT)
- **NOT MATCHED BY SOURCE**: Actions for target rows with no source match (typically UPDATE/DELETE)

## Parameters / Member Variables
- `pstate`: Parser state containing parsing context, namespace information, and accumulated state
- `stmt`: The parsed MERGE statement AST node containing all clause information

## Dependencies
- Functions called/Symbols referenced:
  - makeNode, setTargetTable, transformFromClause
  - transformWithClause, transformExpr, transformWhereClause
  - transformReturningList, transformUpdateTargetList, transformInsertRow
  - checkInsertTargets, transformExpressionList
  - setNamespaceForMergeWhen, addNSItemToQuery
  - GetNSItemByRangeTablePosn, makeFromExpr, makeTargetEntry
  - assign_query_collations, errdetail_relkind_not_supported
  - Various ACL_* permission constants and CMD_* command type constants
- Called from (representative examples):
  - transformStmt (main statement transformation dispatcher)

## Notes and Other Information
- Validates that MERGE can only be performed on tables, partitioned tables, and views
- Handles WITH clauses but prohibits WITH RECURSIVE for MERGE statements
- Creates separate target lists for each action type (INSERT, UPDATE, DELETE, NOTHING)
- Manages complex namespace visibility rules where different actions can see different relations
- Supports INSERT DEFAULT VALUES syntax within MERGE statements
- Includes comprehensive permission checking to ensure all required privileges are validated
- The function creates a complete Query structure but leaves the actual join construction to later phases via transform_MERGE_to_join
- RETURNING clause processing is supported for MERGE statements
- Unreachable WHEN clause detection prevents logical errors in MERGE statement definitions