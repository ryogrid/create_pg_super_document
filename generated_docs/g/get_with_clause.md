# get_with_clause

## Location
[src/backend/utils/adt/ruleutils.c:5563-5701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5563-L5701)

## Overview
Reconstructs the WITH clause (Common Table Expressions/CTEs) from internal representation back to readable SQL text, including support for recursive CTEs and advanced features like SEARCH and CYCLE clauses.

## Definition

```c
static void
get_with_clause(Query *query, deparse_context *context)
```
## Detailed Description
The  function is responsible for converting PostgreSQL's internal representation of WITH clauses (Common Table Expressions) back into standard SQL syntax. It handles both non-recursive and recursive CTEs, along with PostgreSQL's advanced CTE features including materialization hints, SEARCH clauses for controlling recursive traversal order, and CYCLE clauses for cycle detection in recursive queries.

The function processes each CTE in the query's cteList, formatting them with proper syntax including:
- CTE name and optional column aliases
- MATERIALIZED/NOT MATERIALIZED hints when specified
- The actual CTE query definition
- SEARCH clauses (BREADTH FIRST or DEPTH FIRST traversal)
- CYCLE clauses for cycle detection with custom mark values

For recursive CTEs, it uses "WITH RECURSIVE" instead of just "WITH". The function handles proper indentation and formatting according to the pretty-printing flags, and calls  recursively to format the nested query definitions within each CTE.

## Parameters / Member Variables
- : Query object containing the CTE list and recursion flag
- : Deparse context containing output buffer and formatting parameters

## Dependencies
- Functions called/Symbols referenced:
  - [quote_identifier](../q/quote_identifier.md)
  - [get_query_def](get_query_def.md)
  - [get_rule_expr](get_rule_expr.md)
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - PRETTY_INDENT
  - PRETTYINDENT_STD
  - CTEMaterializeDefault, CTEMaterializeAlways, CTEMaterializeNever
  - CommonTableExpr
  - castNode
  - [DatumGetBool](../D/DatumGetBool.md)
- Called from (representative examples):
  - [get_select_query_def](get_select_query_def.md)
  - [get_insert_query_def](get_insert_query_def.md)
  - [get_update_query_def](get_update_query_def.md)
  - [get_delete_query_def](get_delete_query_def.md)
  - [get_merge_query_def](get_merge_query_def.md)

## Notes and Other Information
This function implements support for PostgreSQL's comprehensive CTE feature set, including SQL:1999 standard recursive CTEs and PostgreSQL-specific extensions. The SEARCH clause allows controlling traversal order in recursive queries (breadth-first vs depth-first), while the CYCLE clause enables automatic cycle detection with customizable mark values. The materialization hints control PostgreSQL's query optimizer behavior for CTE evaluation. The function carefully handles proper SQL syntax generation, including comma separation between multiple CTEs and correct parentheses placement around nested queries and column lists.

## Simplified Source

```c
static void get_with_clause(Query *query, deparse_context *context) {
    StringInfo buf = context->buf;
    const char *sep;
    ListCell *l;

    if (query->cteList == NIL)
        return;

    // Handle indentation if pretty printing
    if (PRETTY_INDENT(context)) {
        context->indentLevel += PRETTYINDENT_STD;
        appendStringInfoChar(buf, ' ');
    }

    // Start WITH clause (recursive or non-recursive)
    sep = query->hasRecursive ? "WITH RECURSIVE " : "WITH ";

    foreach(l, query->cteList) {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(l);

        appendStringInfoString(buf, sep);
        appendStringInfoString(buf, quote_identifier(cte->ctename));

        // Add column aliases if present
        if (cte->aliascolnames) {
            bool first = true;
            ListCell *col;

            appendStringInfoChar(buf, '(');
            foreach(col, cte->aliascolnames) {
                if (first)
                    first = false;
                else
                    appendStringInfoString(buf, ", ");
                appendStringInfoString(buf, quote_identifier(strVal(lfirst(col))));
            }
            appendStringInfoChar(buf, ')');
        }

        // Add AS clause with materialization hint
        appendStringInfoString(buf, " AS ");
        switch (cte->ctematerialized) {
            case CTEMaterializeAlways:
                appendStringInfoString(buf, "MATERIALIZED ");
                break;
            case CTEMaterializeNever:
                appendStringInfoString(buf, "NOT MATERIALIZED ");
                break;
            case CTEMaterializeDefault:
            default:
                break;
        }

        // Format CTE query
        appendStringInfoChar(buf, '(');
        if (PRETTY_INDENT(context))
            appendContextKeyword(context, "", 0, 0, 0);
        get_query_def((Query *) cte->ctequery, buf, context->namespaces, NULL,
                     true, context->prettyFlags, context->wrapColumn, context->indentLevel);
        if (PRETTY_INDENT(context))
            appendContextKeyword(context, "", 0, 0, 0);
        appendStringInfoChar(buf, ')');

        // Add SEARCH clause if present
        if (cte->search_clause) {
            bool first = true;
            ListCell *lc;

            appendStringInfo(buf, " SEARCH %s FIRST BY ",
                           cte->search_clause->search_breadth_first ? "BREADTH" : "DEPTH");

            foreach(lc, cte->search_clause->search_col_list) {
                if (first)
                    first = false;
                else
                    appendStringInfoString(buf, ", ");
                appendStringInfoString(buf, quote_identifier(strVal(lfirst(lc))));
            }
            appendStringInfo(buf, " SET %s", quote_identifier(cte->search_clause->search_seq_column));
        }

        // Add CYCLE clause if present
        if (cte->cycle_clause) {
            bool first = true;
            ListCell *lc;

            appendStringInfoString(buf, " CYCLE ");
            foreach(lc, cte->cycle_clause->cycle_col_list) {
                if (first)
                    first = false;
                else
                    appendStringInfoString(buf, ", ");
                appendStringInfoString(buf, quote_identifier(strVal(lfirst(lc))));
            }

            appendStringInfo(buf, " SET %s", quote_identifier(cte->cycle_clause->cycle_mark_column));

            // Add custom mark values if not default boolean values
            Const *cmv = castNode(Const, cte->cycle_clause->cycle_mark_value);
            Const *cmd = castNode(Const, cte->cycle_clause->cycle_mark_default);

            if (!(cmv->consttype == BOOLOID && !cmv->constisnull && DatumGetBool(cmv->constvalue) == true &&
                  cmd->consttype == BOOLOID && !cmd->constisnull && DatumGetBool(cmd->constvalue) == false)) {
                appendStringInfoString(buf, " TO ");
                get_rule_expr(cte->cycle_clause->cycle_mark_value, context, false);
                appendStringInfoString(buf, " DEFAULT ");
                get_rule_expr(cte->cycle_clause->cycle_mark_default, context, false);
            }

            appendStringInfo(buf, " USING %s", quote_identifier(cte->cycle_clause->cycle_path_column));
        }

        sep = ", ";
    }

    // Restore indentation
    if (PRETTY_INDENT(context)) {
        context->indentLevel -= PRETTYINDENT_STD;
        appendContextKeyword(context, "", 0, 0, 0);
    } else {
        appendStringInfoChar(buf, ' ');
    }
}
```