# transformStmt

## Location
[src/backend/parser/analyze.c:311-440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L311-L440)

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
  - [test_raw_expression_coverage](test_raw_expression_coverage.md) (optional testing function)
  - [transformInsertStmt](transformInsertStmt.md), transformDeleteStmt, transformUpdateStmt, transformMergeStmt (DML transformations)
  - [transformSelectStmt](transformSelectStmt.md), transformValuesClause, transformSetOperationStmt (SELECT variations)
  - [transformReturnStmt](transformReturnStmt.md), transformPLAssignStmt (procedural statements)
  - [transformDeclareCursorStmt](transformDeclareCursorStmt.md), transformExplainStmt, transformCreateTableAsStmt, transformCallStmt (special cases)
  - CMD_UTILITY, QSRC_ORIGINAL (constants for Query node initialization)

- Called from (representative examples):
  - [transformOptionalSelectInto](transformOptionalSelectInto.md) (top-level statement processing)
  - [parse_sub_analyze](../p/parse_sub_analyze.md) (subquery analysis)
  - [transformInsertStmt](transformInsertStmt.md) (nested statement transformation)
  - [transformCreateTableAsStmt](transformCreateTableAsStmt.md) (CREATE TABLE AS query transformation)
  - [transformRuleStmt](transformRuleStmt.md) (rule statement processing)

## Notes and Other Information
- The function includes a caution comment noting that changes to statement type handling should be coordinated with stmt_requires_parse_analysis() and analyze_requires_snapshot()
- All transformed queries are marked with querySource = QSRC_ORIGINAL and canSetTag = true by default
- The function serves as the primary entry point for recursive statement transformation throughout the parser
- Different statement types require different levels of semantic analysis, from simple wrapping for utility statements to complex optimization preparation for DML statements

## Simplified Source

```c
Query *
transformStmt(ParseState *pstate, Node *parseTree) {
    Query *result;

    // Optional expression coverage testing for DML statements
#ifdef RAW_EXPRESSION_COVERAGE_TEST
    switch (nodeTag(parseTree)) {
        case T_SelectStmt:
        case T_InsertStmt:
        case T_UpdateStmt:
        case T_DeleteStmt:
        case T_MergeStmt:
            (void) test_raw_expression_coverage(parseTree, NULL);
            break;
        default:
            break;
    }
#endif

    // Main transformation switch statement
    switch (nodeTag(parseTree)) {
        // Optimizable DML statements
        case T_InsertStmt:
            result = transformInsertStmt(pstate, (InsertStmt *) parseTree);
            break;

        case T_DeleteStmt:
            result = transformDeleteStmt(pstate, (DeleteStmt *) parseTree);
            break;

        case T_UpdateStmt:
            result = transformUpdateStmt(pstate, (UpdateStmt *) parseTree);
            break;

        case T_MergeStmt:
            result = transformMergeStmt(pstate, (MergeStmt *) parseTree);
            break;

        case T_SelectStmt: {
            SelectStmt *n = (SelectStmt *) parseTree;

            if (n->valuesLists) {
                result = transformValuesClause(pstate, n);
            } else if (n->op == SETOP_NONE) {
                result = transformSelectStmt(pstate, n);
            } else {
                result = transformSetOperationStmt(pstate, n);
            }
            break;
        }

        // Procedural statements
        case T_ReturnStmt:
            result = transformReturnStmt(pstate, (ReturnStmt *) parseTree);
            break;

        case T_PLAssignStmt:
            result = transformPLAssignStmt(pstate, (PLAssignStmt *) parseTree);
            break;

        // Special case statements that need transformation
        case T_DeclareCursorStmt:
            result = transformDeclareCursorStmt(pstate, (DeclareCursorStmt *) parseTree);
            break;

        case T_ExplainStmt:
            result = transformExplainStmt(pstate, (ExplainStmt *) parseTree);
            break;

        case T_CreateTableAsStmt:
            result = transformCreateTableAsStmt(pstate, (CreateTableAsStmt *) parseTree);
            break;

        case T_CallStmt:
            result = transformCallStmt(pstate, (CallStmt *) parseTree);
            break;

        default:
            // Utility statements - wrap the original parse tree
            result = makeNode(Query);
            result->commandType = CMD_UTILITY;
            result->utilityStmt = (Node *) parseTree;
            break;
    }

    // Mark query metadata
    result->querySource = QSRC_ORIGINAL;
    result->canSetTag = true;

    return result;
}
```