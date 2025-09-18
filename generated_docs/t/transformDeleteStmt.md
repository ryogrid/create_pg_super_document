# transformDeleteStmt

## Location
src/backend/parser/analyze.c: 508 - 579

## Overview
Transforms a raw DELETE statement parse tree into a fully analyzed Query tree ready for optimization and execution.

## Definition
```c
static Query *transformDeleteStmt(ParseState *pstate, DeleteStmt *stmt)
```

## Detailed Description
This function performs comprehensive semantic analysis and transformation of DELETE statements, converting the raw parse tree into an optimizable Query structure. The transformation process handles all aspects of DELETE statement semantics including:

1. **WITH clause processing** - Handles Common Table Expressions (CTEs) including recursive CTEs
2. **Target table setup** - Establishes the result relation with appropriate permissions checking
3. **USING clause transformation** - Processes the PostgreSQL-specific USING clause (equivalent to FROM in UPDATE)
4. **WHERE clause analysis** - Transforms filtering conditions with proper scoping
5. **RETURNING clause processing** - Handles optional RETURNING expressions for output
6. **Range table construction** - Builds complete range table and join tree
7. **Semantic validation** - Performs aggregate function checking and other semantic constraints

The function carefully manages namespace visibility, ensuring that subqueries in the USING clause cannot reference the target table, while allowing normal references in WHERE and RETURNING clauses.

## Parameters / Member Variables
- : ParseState context containing parsing state, range tables, namespace information, and error reporting context
- : DeleteStmt parse tree node containing the raw DELETE statement structure

## Dependencies
- Functions called/Symbols referenced:
  - DeleteStmt, ParseNamespaceItem (structure types)
  - CMD_DELETE (command type constant)
  - transformWithClause (CTE processing)
  - setTargetTable (target relation setup with ACL_DELETE permission)
  - transformFromClause (USING clause processing)
  - transformWhereClause (WHERE condition analysis with EXPR_KIND_WHERE context)
  - transformReturningList (RETURNING expressions with EXPR_KIND_RETURNING context)
  - makeFromExpr (join tree construction)
  - assign_query_collations (collation assignment)
  - parseCheckAggregates (aggregate function validation)

- Called from (representative examples):
  - transformStmt (main statement transformation dispatcher)

## Notes and Other Information
- The USING clause is PostgreSQL-specific SQL syntax that provides FROM-list functionality for DELETE statements (FROM is already a keyword in DELETE syntax)
- The function implements careful scoping rules: USING subqueries cannot access the target relation, but WHERE and RETURNING clauses can
- Namespace item lateral access is temporarily restricted during USING clause processing to prevent improper references
- The transformation preserves all semantic information needed for query optimization including sublinks, window functions, set-returning functions, and aggregates
- Collation assignment must be completed before aggregate checking for reliable expression comparison
- The function sets distinctClause to NIL as DELETE statements do not support DISTINCT operations