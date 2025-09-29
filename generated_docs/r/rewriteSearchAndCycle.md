# rewriteSearchAndCycle

## Location
[src/backend/rewrite/rewriteSearchCycle.c:203-681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteSearchCycle.c#L203-L681)

## Overview
Rewrites a Common Table Expression (CTE) with SEARCH or CYCLE clauses into an equivalent recursive CTE with additional columns for tracking traversal path and detecting cycles.

## Definition
```c
CommonTableExpr *rewriteSearchAndCycle(CommonTableExpr *cte)
```

## Detailed Description
This function is the main entry point for transforming recursive CTEs that have SEARCH and/or CYCLE clauses into standard recursive CTEs with appropriate path tracking mechanisms. It handles both SEARCH clauses (for breadth-first and depth-first traversal ordering) and CYCLE clauses (for cycle detection and prevention).

The function performs a comprehensive rewrite of the CTE structure:

For SEARCH clauses:
- BREADTH FIRST: Adds a search sequence column containing ROW(depth, search_columns) to track traversal depth
- DEPTH FIRST: Adds a search sequence column containing an array of ROW(search_columns) to track traversal path

For CYCLE clauses:
- Adds a cycle mark column with boolean-like values to indicate cycle detection
- Adds a cycle path column containing an array of ROW(cycle_columns) to track the path for cycle detection
- Modifies the recursive query to include a WHERE condition that prevents further traversal when a cycle is detected

The rewriting process involves:
1. Parsing the original CTE's UNION structure to identify base and recursive queries
2. Creating new left subquery (base case) with initialized path tracking columns
3. Creating new right subquery (recursive case) with path accumulation and cycle detection logic
4. Updating the SetOperationStmt and CTE metadata to include the new columns

## Parameters / Member Variables
- `cte`: Pointer to the CommonTableExpr to be rewritten, which must have either a search_clause, cycle_clause, or both

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (to create deep copies of AST nodes)
  - castNode (for type-safe casting)
  - rt_fetch (to access range table entries)
  - makeNode (to create new AST nodes)
  - [makeAlias](../m/makeAlias.md) (to create table aliases)
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md) (to adjust variable references for sublevel changes)
  - [make_path_rowexpr](../m/make_path_rowexpr.md) (to create row expressions for path tracking)
  - [make_path_initial_array](../m/make_path_initial_array.md) (to create initial path arrays)
  - [make_path_cat_expr](../m/make_path_cat_expr.md) (to create path concatenation expressions)
  - [makeVar](../m/makeVar.md), makeTargetEntry, makeFuncExpr (AST construction functions)
  - Various list manipulation functions (lappend, list_make1, etc.)
- Called from:
  - [fireRIRrules](../f/fireRIRrules.md) (in rewriteHandler.c at line 2000)

## Notes and Other Information
- This function is the core implementation of the PostgreSQL SEARCH and CYCLE clause feature for recursive CTEs
- The rewritten CTE maintains the same external interface but adds internal columns for tracking
- Supports both individual SEARCH or CYCLE clauses and combinations of both
- Includes comprehensive error checking for unsupported recursive CTE structures
- The function modifies multiple levels of the query structure including the CTE definition, subqueries, target lists, and column metadata
- [Path](../P/Path.md) tracking uses PostgreSQL's record and record array types for efficient storage and comparison
- For cycle detection, uses scalar array operations (= ANY) for efficient path membership testing

## Simplified Source

```c
CommonTableExpr *rewriteSearchAndCycle(CommonTableExpr *cte) {
    // Copy the CTE for modification
    cte = copyObject(cte);
    ctequery = castNode(Query, cte->ctequery);

    // Extract the UNION structure (base query and recursive query)
    sos = castNode(SetOperationStmt, ctequery->setOperations);
    rti1 = castNode(RangeTblRef, sos->larg)->rtindex;  // base query
    rti2 = castNode(RangeTblRef, sos->rarg)->rtindex;  // recursive query
    rte1 = rt_fetch(rti1, ctequery->rtable);
    rte2 = rt_fetch(rti2, ctequery->rtable);

    // Calculate column positions for new tracking columns
    if (cte->search_clause)
        sqc_attno = list_length(cte->ctecolnames) + 1;
    if (cte->cycle_clause) {
        cmc_attno = list_length(cte->ctecolnames) + 1;
        cpa_attno = list_length(cte->ctecolnames) + 2;
        if (cte->search_clause) {
            cmc_attno++;
            cpa_attno++;
        }
    }

    // Rewrite base query (left side of UNION)
    newq1 = makeNode(Query);
    // Copy original columns and add initial tracking columns
    for (int i = 0; i < list_length(cte->ctecolnames); i++) {
        // Add regular column
        var = makeVar(1, i + 1, ...);
        tle = makeTargetEntry((Expr *) var, i + 1, ...);
        newq1->targetList = lappend(newq1->targetList, tle);
    }

    // Add SEARCH column initialization
    if (cte->search_clause) {
        search_col_rowexpr = make_path_rowexpr(cte, cte->search_clause->search_col_list);
        if (breadth_first)
            texpr = (Expr *) search_col_rowexpr;  // ROW(0, search_cols)
        else
            texpr = make_path_initial_array(search_col_rowexpr);  // ARRAY[ROW(search_cols)]
        tle = makeTargetEntry(texpr, ..., cte->search_clause->search_seq_column, false);
        newq1->targetList = lappend(newq1->targetList, tle);
    }

    // Add CYCLE column initialization
    if (cte->cycle_clause) {
        // Add cycle mark column (default value)
        tle = makeTargetEntry((Expr *) cte->cycle_clause->cycle_mark_default, ...);
        newq1->targetList = lappend(newq1->targetList, tle);

        // Add cycle path column (initial array)
        cycle_col_rowexpr = make_path_rowexpr(cte, cte->cycle_clause->cycle_col_list);
        tle = makeTargetEntry(make_path_initial_array(cycle_col_rowexpr), ...);
        newq1->targetList = lappend(newq1->targetList, tle);
    }

    // Rewrite recursive query (right side of UNION)
    newq2 = makeNode(Query);

    // Find CTE reference in recursive query
    for (int rti = 1; rti <= list_length(rte2->subquery->rtable); rti++) {
        RangeTblEntry *e = rt_fetch(rti, rte2->subquery->rtable);
        if (e->rtekind == RTE_CTE && strcmp(cte->ctename, e->ctename) == 0)
            cte_rtindex = rti;
    }

    // Copy original columns
    for (int i = 0; i < list_length(cte->ctecolnames); i++) {
        // Add regular column
    }

    // Add SEARCH column update
    if (cte->search_clause) {
        if (breadth_first)
            texpr = ROW(sqc.depth + 1, cols);  // Increment depth
        else
            texpr = sqc || ARRAY[ROW(cols)];   // Append to path
        tle = makeTargetEntry(texpr, ...);
        newq2->targetList = lappend(newq2->targetList, tle);
    }

    // Add CYCLE column update and detection
    if (cte->cycle_clause) {
        // Add cycle detection condition to WHERE clause
        expr = make_opclause(...);  // cmc <> cmv condition
        newq2->jointree = makeFromExpr(..., (Node *) expr);

        // Add cycle mark column: CASE WHEN ROW(cols) = ANY(cpa) THEN cmv ELSE cmd END
        saoe = makeNode(ScalarArrayOpExpr);  // ROW(cols) = ANY(cpa)
        caseexpr = makeNode(CaseExpr);       // CASE ... END
        tle = makeTargetEntry((Expr *) caseexpr, ...);
        newq2->targetList = lappend(newq2->targetList, tle);

        // Add cycle path column: cpa || ARRAY[ROW(cols)]
        tle = makeTargetEntry(make_path_cat_expr(cycle_col_rowexpr, cpa_attno), ...);
        newq2->targetList = lappend(newq2->targetList, tle);
    }

    // Update SetOperationStmt metadata
    if (cte->search_clause) {
        sos->colTypes = lappend_oid(sos->colTypes, search_seq_type);
        // Update other metadata...
    }
    if (cte->cycle_clause) {
        sos->colTypes = lappend_oid(sos->colTypes, cte->cycle_clause->cycle_mark_type);
        sos->colTypes = lappend_oid(sos->colTypes, RECORDARRAYOID);
        // Update other metadata...
    }

    // Update CTE metadata
    cte->ctecolnames = ewcl;
    if (cte->search_clause) {
        cte->ctecoltypes = lappend_oid(cte->ctecoltypes, search_seq_type);
        // Update other metadata...
    }
    if (cte->cycle_clause) {
        cte->ctecoltypes = lappend_oid(cte->ctecoltypes, cte->cycle_clause->cycle_mark_type);
        cte->ctecoltypes = lappend_oid(cte->ctecoltypes, RECORDARRAYOID);
        // Update other metadata...
    }

    return cte;
}
```