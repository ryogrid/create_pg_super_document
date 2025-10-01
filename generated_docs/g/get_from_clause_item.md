# get_from_clause_item

## Location
[src/backend/utils/adt/ruleutils.c:12034-12324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12034-L12324)

## Overview
Generates SQL text representation of a single FROM clause item (table, subquery, function, join, etc.) for query deparsing.

## Definition
```c
static void get_from_clause_item(Node *jtnode, Query *query, deparse_context *context)
```

## Detailed Description
This function is a core component of PostgreSQL's query deparsing system, responsible for converting internal join tree nodes back into SQL text format. It handles various types of FROM clause items including regular tables, subqueries, functions, table functions, VALUES clauses, CTEs, and complex joins.

The function operates by examining the node type and dispatching to appropriate handling logic:
- For RangeTblRef nodes, it processes individual range table entries (RTE) based on their kind (relation, subquery, function, etc.)
- For JoinExpr nodes, it recursively processes left and right join arguments and handles join conditions
- Each case generates appropriate SQL syntax including table names, aliases, join keywords, and conditions

Special handling is provided for:
- LATERAL queries with lateral keyword emission
- Function RTEs with complex ROWS FROM() syntax and UNNEST optimization
- Subqueries with proper parenthesization
- Join expressions with correct precedence and aliasing
- Column definition lists and tablesample clauses

## Parameters / Member Variables
- `jtnode`: Node pointer to the join tree node (either RangeTblRef or JoinExpr)
- `query`: Query structure containing the range table and other query information
- `context`: Deparse context containing output buffer, namespace information, and formatting options

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - deparse_columns_fetch
  - [generate_relation_name](generate_relation_name.md)
  - [get_query_def](get_query_def.md)
  - [get_rule_expr_funccall](get_rule_expr_funccall.md)
  - [get_tablefunc](get_tablefunc.md)
  - [get_values_def](get_values_def.md)
  - [get_rte_alias](get_rte_alias.md)
  - [get_column_alias_list](get_column_alias_list.md)
  - [get_from_clause_coldeflist](get_from_clause_coldeflist.md)
  - [get_tablesample_def](get_tablesample_def.md)
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [get_rule_expr](get_rule_expr.md)
- Called from (representative examples):
  - [get_from_clause](get_from_clause.md)
  - [get_from_clause_item](get_from_clause_item.md) (recursive calls for join processing)

## Notes and Other Information
- This is a recursive function that calls itself when processing join expressions
- The function maintains proper SQL syntax formatting including parentheses, commas, and keywords
- Special optimization logic exists for UNNEST functions to collapse multiple UNNEST calls back to standard syntax
- Handles both pretty-printed and compact output formats based on context settings
- Critical for PostgreSQL's ability to display query plans and rules in human-readable SQL format

## Simplified Source

```c
static void
get_from_clause_item(Node *jtnode, Query *query, deparse_context *context)
{
    StringInfo buf = context->buf;
    deparse_namespace *dpns = (deparse_namespace *) linitial(context->namespaces);

    if (IsA(jtnode, RangeTblRef))
    {
        int varno = ((RangeTblRef *) jtnode)->rtindex;
        RangeTblEntry *rte = rt_fetch(varno, query->rtable);
        deparse_columns *colinfo = deparse_columns_fetch(varno, dpns);

        // Handle LATERAL keyword
        if (rte->lateral)
            appendStringInfoString(buf, "LATERAL ");

        // Generate SQL based on RTE type
        switch (rte->rtekind)
        {
            case RTE_RELATION:
                // Regular table: [ONLY] table_name
                appendStringInfo(buf, "%s%s",
                                only_marker(rte),
                                generate_relation_name(rte->relid, context->namespaces));
                break;

            case RTE_SUBQUERY:
                // Subquery: (SELECT ...)
                appendStringInfoChar(buf, '(');
                get_query_def(rte->subquery, buf, context->namespaces, NULL,
                              true, context->prettyFlags, context->wrapColumn,
                              context->indentLevel);
                appendStringInfoChar(buf, ')');
                break;

            case RTE_FUNCTION:
                // Function call or ROWS FROM(...)
                // Simplified: just handle basic function calls
                get_rule_expr_funccall(((RangeTblFunction *) linitial(rte->functions))->funcexpr,
                                       context, true);
                if (rte->funcordinality)
                    appendStringInfoString(buf, " WITH ORDINALITY");
                break;

            case RTE_TABLEFUNC:
                get_tablefunc(rte->tablefunc, context, true);
                break;

            case RTE_VALUES:
                // VALUES clause: (VALUES (...), (...))
                appendStringInfoChar(buf, '(');
                get_values_def(rte->values_lists, context);
                appendStringInfoChar(buf, ')');
                break;

            case RTE_CTE:
                // Common Table Expression
                appendStringInfoString(buf, quote_identifier(rte->ctename));
                break;

            default:
                elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
                break;
        }

        // Add alias and column definitions
        get_rte_alias(rte, varno, false, context);
        get_column_alias_list(colinfo, context);

        // Add tablesample clause if present
        if (rte->rtekind == RTE_RELATION && rte->tablesample)
            get_tablesample_def(rte->tablesample, context);
    }
    else if (IsA(jtnode, JoinExpr))
    {
        JoinExpr *j = (JoinExpr *) jtnode;
        deparse_columns *colinfo = deparse_columns_fetch(j->rtindex, dpns);

        // Add parentheses if needed
        if (!PRETTY_PAREN(context) || j->alias != NULL)
            appendStringInfoChar(buf, '(');

        // Process left side of join
        get_from_clause_item(j->larg, query, context);

        // Add appropriate join keyword
        switch (j->jointype)
        {
            case JOIN_INNER:
                if (j->quals)
                    appendContextKeyword(context, " JOIN ", -PRETTYINDENT_STD,
                                        PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                else
                    appendContextKeyword(context, " CROSS JOIN ", -PRETTYINDENT_STD,
                                        PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                break;
            case JOIN_LEFT:
                appendContextKeyword(context, " LEFT JOIN ", -PRETTYINDENT_STD,
                                    PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                break;
            case JOIN_FULL:
                appendContextKeyword(context, " FULL JOIN ", -PRETTYINDENT_STD,
                                    PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                break;
            case JOIN_RIGHT:
                appendContextKeyword(context, " RIGHT JOIN ", -PRETTYINDENT_STD,
                                    PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                break;
            default:
                elog(ERROR, "unrecognized join type: %d", (int) j->jointype);
        }

        // Process right side of join
        get_from_clause_item(j->rarg, query, context);

        // Add join conditions
        if (j->usingClause)
        {
            // USING clause
            appendStringInfoString(buf, " USING (");
            // Add column names from usingNames
            get_column_list_from_using(colinfo->usingNames, buf);
            appendStringInfoChar(buf, ')');

            if (j->join_using_alias)
                appendStringInfo(buf, " AS %s",
                                quote_identifier(j->join_using_alias->aliasname));
        }
        else if (j->quals)
        {
            // ON clause
            appendStringInfoString(buf, " ON ");
            get_rule_expr(j->quals, context, false);
        }
        else if (j->jointype != JOIN_INNER)
        {
            // Natural join or similar - add ON TRUE
            appendStringInfoString(buf, " ON TRUE");
        }

        // Close parentheses and add alias
        if (!PRETTY_PAREN(context) || j->alias != NULL)
            appendStringInfoChar(buf, ')');

        if (j->alias != NULL)
        {
            appendStringInfo(buf, " %s",
                            quote_identifier(get_rtable_name(j->rtindex, context)));
            get_column_alias_list(colinfo, context);
        }
    }
    else
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(jtnode));
}
```