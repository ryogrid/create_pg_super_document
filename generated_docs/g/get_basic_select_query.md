# get_basic_select_query

## Location
[src/backend/utils/adt/ruleutils.c:5904-6034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5904-L6034)

## Overview
Generates the basic structure of a SELECT query string by building each clause (SELECT, DISTINCT, FROM, WHERE, GROUP BY, HAVING, WINDOW) in the proper SQL format.

## Definition
```c
static void get_basic_select_query(Query *query, deparse_context *context)
```

## Detailed Description
This function constructs a textual representation of a basic SELECT query by processing each SQL clause in order. It first checks if the query can be simplified to a VALUES clause using get_simple_values_rte(). If not, it systematically builds the query string starting with SELECT/RETURN, followed by DISTINCT clause, target list, FROM clause, WHERE clause, GROUP BY clause (including grouping sets), HAVING clause, and WINDOW clause.

The function handles various SQL features including:
- DISTINCT and DISTINCT ON clauses
- Regular and RETURN-style SELECT statements  
- GROUP BY with grouping sets and DISTINCT grouping
- Proper formatting and indentation based on context settings
- Special handling for VALUES clauses that can be simplified

## Parameters / Member Variables
- `query`: The Query structure containing the parsed SELECT statement to deparse
- `context`: The deparse_context containing formatting options, buffer, and state information

## Dependencies
- Functions called/Symbols referenced:
  - [get_simple_values_rte](get_simple_values_rte.md) (check for simple VALUES pattern)
  - [get_values_def](get_values_def.md) (generate VALUES clause)
  - [get_target_list](get_target_list.md) (generate SELECT target list)
  - [get_from_clause](get_from_clause.md) (generate FROM clause)
  - [get_rule_expr](get_rule_expr.md) (generate WHERE/HAVING expressions)
  - [get_rule_sortgroupclause](get_rule_sortgroupclause.md) (generate GROUP BY/DISTINCT ON items)
  - [get_rule_groupingset](get_rule_groupingset.md) (generate grouping sets)
  - [get_rule_windowclause](get_rule_windowclause.md) (generate WINDOW clause)
  - [appendContextKeyword](../a/appendContextKeyword.md) (format SQL keywords with proper spacing)
- Called from (representative examples):
  - [get_select_query_def](get_select_query_def.md) (src/backend/utils/adt/ruleutils.c:5728)

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for converting internal Query structures back to SQL text
- Handles both regular SELECT and RETURN statements (for SQL functions)
- Maintains proper SQL formatting and indentation through the deparse_context
- Optimizes simple VALUES patterns by bypassing full SELECT structure when possible
- Properly manages context state like inGroupBy flag to ensure correct expression formatting

## Simplified Source

```c
static void
get_basic_select_query(Query *query, deparse_context *context)
{
    StringInfo buf = context->buf;

    // Handle pretty indentation
    if (PRETTY_INDENT(context)) {
        context->indentLevel += PRETTYINDENT_STD;
        appendStringInfoChar(buf, ' ');
    }

    // Check for simple VALUES clause optimization
    RangeTblEntry *values_rte = get_simple_values_rte(query, context->resultDesc);
    if (values_rte) {
        get_values_def(values_rte->values_lists, context);
        return;
    }

    // Build SELECT or RETURN keyword
    if (query->isReturn)
        appendStringInfoString(buf, "RETURN");
    else
        appendStringInfoString(buf, "SELECT");

    // Add DISTINCT clause if present
    if (query->distinctClause != NIL) {
        if (query->hasDistinctOn) {
            appendStringInfoString(buf, " DISTINCT ON (");
            // Add distinct expressions with commas
            char *sep = "";
            foreach(l, query->distinctClause) {
                SortGroupClause *srt = (SortGroupClause *) lfirst(l);
                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(srt->tleSortGroupRef, query->targetList, false, context);
                sep = ", ";
            }
            appendStringInfoChar(buf, ')');
        } else {
            appendStringInfoString(buf, " DISTINCT");
        }
    }

    // Generate target list (what to select)
    get_target_list(query->targetList, context);

    // Add FROM clause
    get_from_clause(query, " FROM ", context);

    // Add WHERE clause
    if (query->jointree->quals != NULL) {
        appendContextKeyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_expr(query->jointree->quals, context, false);
    }

    // Add GROUP BY clause
    if (query->groupClause != NULL || query->groupingSets != NULL) {
        appendContextKeyword(context, " GROUP BY ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);

        if (query->groupDistinct)
            appendStringInfoString(buf, "DISTINCT ");

        bool save_ingroupby = context->inGroupBy;
        context->inGroupBy = true;

        if (query->groupingSets == NIL) {
            // Regular GROUP BY
            char *sep = "";
            foreach(l, query->groupClause) {
                SortGroupClause *grp = (SortGroupClause *) lfirst(l);
                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(grp->tleSortGroupRef, query->targetList, false, context);
                sep = ", ";
            }
        } else {
            // GROUPING SETS
            char *sep = "";
            foreach(l, query->groupingSets) {
                GroupingSet *grp = lfirst(l);
                appendStringInfoString(buf, sep);
                get_rule_groupingset(grp, query->targetList, true, context);
                sep = ", ";
            }
        }

        context->inGroupBy = save_ingroupby;
    }

    // Add HAVING clause
    if (query->havingQual != NULL) {
        appendContextKeyword(context, " HAVING ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        get_rule_expr(query->havingQual, context, false);
    }

    // Add WINDOW clause
    if (query->windowClause != NIL)
        get_rule_windowclause(query, context);
}
```