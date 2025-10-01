# analyzeCTE

## Location
[src/backend/parser/parse_cte.c:243-570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L243-L570)

## Overview
Performs the actual parse analysis transformation of one Common Table Expression (CTE), handling column type validation, SEARCH/CYCLE clause processing, and recursive CTE verification.

## Definition

```c
static void
analyzeCTE(ParseState *pstate, CommonTableExpr *cte)
```
## Detailed Description
This static function transforms a single CTE from its raw parsed form into an analyzed Query node. It handles several complex aspects of CTE processing:

1. **CYCLE clause preprocessing**: Determines data types for cycle mark values and validates operators before query analysis
2. **Query analysis**: Uses parse_sub_analyze to transform the CTE's query into its internal representation
3. **Type validation for recursive CTEs**: Ensures output column types and collations match between recursive and non-recursive terms
4. **SEARCH/CYCLE clause validation**: Verifies that SEARCH and CYCLE clauses are properly formed and reference valid columns
5. **Expandability checks**: Ensures recursive CTEs with SEARCH/CYCLE clauses meet SQL standard requirements

The function performs extensive error checking and provides detailed error messages for various invalid CTE constructs.

## Parameters / Member Variables
- : Parse state containing context information for error reporting and CTE namespace management
- : The CommonTableExpr node to be analyzed and transformed

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](../t/transformExpr.md) - transforms cycle mark expressions
  - [select_common_type](../s/select_common_type.md) - determines common type for cycle mark values
  - [coerce_to_common_type](../c/coerce_to_common_type.md) - coerces expressions to common type
  - [parse_sub_analyze](../p/parse_sub_analyze.md) - performs main query analysis
  - [analyzeCTETargetList](analyzeCTETargetList.md) - analyzes CTE output column specifications
  - GetCTETargetList - retrieves target list from CTE
  - [lookup_type_cache](../l/lookup_type_cache.md) - looks up type operators for cycle detection
  - [get_negator](../g/get_negator.md) - finds inequality operator for cycle mark comparison
- Called from (representative examples):
  - [transformWithClause](../t/transformWithClause.md) - called for each CTE in recursive WITH processing
  - [transformWithClause](../t/transformWithClause.md) - called for each CTE in non-recursive WITH processing

## Notes and Other Information
- The function is static and only used within parse_cte.c
- Handles both recursive and non-recursive CTEs with different validation logic
- For recursive CTEs, validates that column types match between terms
- SEARCH and CYCLE clauses are only allowed on recursive CTEs
- Data-modifying CTEs are only allowed at the top level of queries
- All CTE queries are marked as canSetTag = false
- Provides comprehensive validation of SEARCH and CYCLE clause column references
- Ensures SQL standard "expandability" requirements for recursive CTEs with special clauses

## Simplified Source

```c
static void
analyzeCTE(ParseState *pstate, CommonTableExpr *cte)
{
    Query *query;
    CTESearchClause *search_clause = cte->search_clause;
    CTECycleClause *cycle_clause = cte->cycle_clause;

    // Process cycle clause if present - determine data types for cycle mark values
    if (cycle_clause) {
        // Transform cycle mark expressions and determine common type
        cycle_clause->cycle_mark_value = transformExpr(pstate, cycle_clause->cycle_mark_value, EXPR_KIND_CYCLE_MARK);
        cycle_clause->cycle_mark_default = transformExpr(pstate, cycle_clause->cycle_mark_default, EXPR_KIND_CYCLE_MARK);

        cycle_clause->cycle_mark_type = select_common_type(pstate,
            list_make2(cycle_clause->cycle_mark_value, cycle_clause->cycle_mark_default),
            "CYCLE", NULL);

        // Coerce values to common type and determine operator
        cycle_clause->cycle_mark_value = coerce_to_common_type(pstate, cycle_clause->cycle_mark_value,
            cycle_clause->cycle_mark_type, "CYCLE/SET/TO");
        cycle_clause->cycle_mark_default = coerce_to_common_type(pstate, cycle_clause->cycle_mark_default,
            cycle_clause->cycle_mark_type, "CYCLE/SET/DEFAULT");

        // Look up inequality operator for cycle detection
        TypeCacheEntry *typentry = lookup_type_cache(cycle_clause->cycle_mark_type, TYPECACHE_EQ_OPR);
        if (!OidIsValid(typentry->eq_opr))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("could not identify an equality operator for type %s", format_type_be(cycle_clause->cycle_mark_type))));
        cycle_clause->cycle_mark_neop = get_negator(typentry->eq_opr);
    }

    // Parse and analyze the CTE's query
    query = parse_sub_analyze(cte->ctequery, pstate, cte, false, true);
    cte->ctequery = (Node *) query;

    // Basic validation
    if (!IsA(query, Query) || query->utilityStmt != NULL)
        elog(ERROR, "unexpected statement type in WITH");

    // Restrict data-modifying CTEs to top level
    if (query->commandType != CMD_SELECT && pstate->parentParseState != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("WITH clause containing a data-modifying statement must be at the top level")));

    query->canSetTag = false;

    // Handle column type analysis
    if (!cte->cterecursive) {
        analyzeCTETargetList(pstate, cte, GetCTETargetList(cte));
    } else {
        // For recursive CTEs, verify output column types match between terms
        ListCell *lctlist, *lctyp, *lctypmod, *lccoll;
        int varattno = 0;

        lctyp = list_head(cte->ctecoltypes);
        lctypmod = list_head(cte->ctecoltypmods);
        lccoll = list_head(cte->ctecolcollations);

        foreach(lctlist, GetCTETargetList(cte)) {
            TargetEntry *te = (TargetEntry *) lfirst(lctlist);
            if (te->resjunk) continue;

            varattno++;
            // Validate type and collation consistency
            if (exprType((Node *) te->expr) != lfirst_oid(lctyp) ||
                exprTypmod((Node *) te->expr) != lfirst_int(lctypmod))
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("recursive query \"%s\" column %d has mismatched types", cte->ctename, varattno)));
        }
    }

    // Validate SEARCH and CYCLE clauses for recursive CTEs
    if (search_clause || cycle_clause) {
        if (!cte->cterecursive)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WITH query is not recursive")));

        // Validate required UNION structure for expandability
        Query *ctequery = castNode(Query, cte->ctequery);
        SetOperationStmt *sos = castNode(SetOperationStmt, ctequery->setOperations);

        if (!IsA(sos->larg, RangeTblRef) || !IsA(sos->rarg, RangeTblRef))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SEARCH/CYCLE clause requires simple UNION structure")));
    }

    // Validate SEARCH clause column references
    if (search_clause) {
        foreach(lc, search_clause->search_col_list) {
            String *colname = lfirst_node(String, lc);
            if (!list_member(cte->ctecolnames, colname))
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("search column \"%s\" not in WITH query column list", strVal(colname))));
        }
    }

    // Validate CYCLE clause column references and uniqueness
    if (cycle_clause) {
        foreach(lc, cycle_clause->cycle_col_list) {
            String *colname = lfirst_node(String, lc);
            if (!list_member(cte->ctecolnames, colname))
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("cycle column \"%s\" not in WITH query column list", strVal(colname))));
        }

        // Ensure mark and path columns don't conflict with existing columns
        if (list_member(cte->ctecolnames, makeString(cycle_clause->cycle_mark_column)) ||
            list_member(cte->ctecolnames, makeString(cycle_clause->cycle_path_column)))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("cycle column names conflict with existing columns")));
    }
}
```