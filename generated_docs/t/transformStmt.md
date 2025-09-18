# transformStmt

## Location
src/backend/parser/analyze.c: 311 - 440

## Overview
The main recursive function that transforms raw parse tree nodes into executable Query trees, serving as the central dispatcher for all PostgreSQL statement types.

## Definition
```c
Query *transformStmt(ParseState *pstate, Node *parseTree)
```

## Detailed Description
transformStmt is the core function of PostgreSQL's semantic analysis phase, responsible for converting raw parse tree nodes into optimizable Query structures. The function operates as a large switch statement that dispatches different statement types to their specialized transformation functions.

The function handles two main categories of statements:
1. **Optimizable statements** - DML operations (SELECT, INSERT, UPDATE, DELETE, MERGE) and procedural statements that can be optimized by the query planner
2. **Utility statements** - DDL and administrative commands that are executed directly without optimization

For optimizable statements, the function calls specialized transformation functions that perform detailed semantic analysis, type checking, and query tree construction. For utility statements, it simply wraps the original parse tree in a Query node with CMD_UTILITY command type.

The function also includes optional raw expression coverage testing for DML statements when compiled with RAW_EXPRESSION_COVERAGE_TEST.

## Parameters / Member Variables
- : ParseState context containing parsing state, range tables, error context, and other semantic analysis information
- : Raw parse tree node representing the statement to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (node type identification)
  - test_raw_expression_coverage (optional testing function)
  - transformInsertStmt, transformDeleteStmt, transformUpdateStmt, transformMergeStmt (DML transformations)
  - transformSelectStmt, transformValuesClause, transformSetOperationStmt (SELECT variations)
  - transformReturnStmt, transformPLAssignStmt (procedural statements)
  - transformDeclareCursorStmt, transformExplainStmt, transformCreateTableAsStmt, transformCallStmt (special cases)
  - CMD_UTILITY, QSRC_ORIGINAL (constants for Query node initialization)

- Called from (representative examples):
  - transformOptionalSelectInto (top-level statement processing)
  - parse_sub_analyze (subquery analysis)
  - transformInsertStmt (nested statement transformation)
  - transformCreateTableAsStmt (CREATE TABLE AS query transformation)
  - transformRuleStmt (rule statement processing)

## Notes and Other Information
- The function includes a caution comment noting that changes to statement type handling should be coordinated with stmt_requires_parse_analysis() and analyze_requires_snapshot()
- All transformed queries are marked with querySource = QSRC_ORIGINAL and canSetTag = true by default
- The function serves as the primary entry point for recursive statement transformation throughout the parser
- Different statement types require different levels of semantic analysis, from simple wrapping for utility statements to complex optimization preparation for DML statements