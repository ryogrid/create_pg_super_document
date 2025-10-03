# get_select_query_def

## Location
[src/backend/utils/adt/ruleutils.c:5702-5834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5702-L5834)

## Overview
Converts a SELECT Query parse tree back into readable SQL text by orchestrating the formatting of all SELECT statement components including WITH, FROM, WHERE, ORDER BY, LIMIT, and locking clauses.

## Definition

```c
static void
get_select_query_def(Query *query, deparse_context *context)
```
## Detailed Description
The  function serves as the main coordinator for reconstructing SELECT statements from PostgreSQL's internal Query representation. It handles the complete SELECT statement structure by processing components in the correct SQL order and delegating to specialized functions for each clause.

The function first processes the WITH clause if present, then determines whether to handle the query as a set operation (UNION/INTERSECT/EXCEPT) or a basic SELECT. For set operations, it calls  to handle the complex tree structure. For basic SELECT statements, it calls  to format the core SELECT components.

After handling the main query body, the function processes the remaining clauses in SQL order:
- ORDER BY clause with proper column number handling for set operations
- LIMIT/OFFSET clauses with support for both traditional LIMIT syntax and SQL standard FETCH FIRST...ROWS WITH TIES
- FOR UPDATE/SHARE locking clauses with various lock strengths and wait policies

The function sets up the deparse context with the query's target list and window clause information, which are needed by various sub-functions for proper name resolution and formatting.

## Parameters / Member Variables
- `*query`: Query parse tree representing the SELECT statement to be formatted
- `*context`: Deparse context containing output buffer, formatting flags, and namespace information
## Dependencies
- Functions called/Symbols referenced:
  - [get_with_clause](get_with_clause.md)
  - [get_setop_query](get_setop_query.md)
  - [get_basic_select_query](get_basic_select_query.md)
  - [get_rule_orderby](get_rule_orderby.md)
  - [get_rule_expr](get_rule_expr.md)
  - [get_rtable_name](get_rtable_name.md)
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - PRETTYINDENT_STD
  - LIMIT_OPTION_WITH_TIES
  - LCS_FORKEYSHARE, LCS_FORSHARE, LCS_FORNOKEYUPDATE, LCS_FORUPDATE, LCS_NONE
  - LockWaitError, LockWaitSkip
  - [RowMarkClause](../R/RowMarkClause.md)
- Called from (representative examples):
  - [get_query_def](get_query_def.md)

## Notes and Other Information
This function is the primary entry point for SELECT statement deparsing within the broader query deparsing system. It demonstrates PostgreSQL's comprehensive SELECT statement support, including advanced features like set operations, window functions (via windowClause context setup), and various locking modes with different wait policies. The function handles both simple and complex SELECT statements, properly formatting set operations where only ORDER BY and LIMIT clauses are meaningful at the top level. The locking clause processing supports all PostgreSQL lock strengths from KEY SHARE to UPDATE, along with NOWAIT and SKIP LOCKED options. The LIMIT clause processing includes support for the SQL standard FETCH FIRST syntax with WITH TIES option, showing PostgreSQL's SQL standards compliance alongside its traditional syntax.

## Simplified Source
```c
static void get_select_query_def(Query *query, deparse_context *context) {
    StringInfo buf = context->buf;
    bool force_colno;

    // Add WITH clause if present
    get_with_clause(query, context);

    // Set context for subroutines
    context->targetList = query->targetList;
    context->windowClause = query->windowClause;

    // Handle set operations vs basic SELECT
    if (query->setOperations) {
        // UNION/INTERSECT/EXCEPT query
        get_setop_query(query->setOperations, query, context);
        force_colno = true;  // ORDER BY must use column numbers
    }
    else {
        // Basic SELECT query
        get_basic_select_query(query, context);
        force_colno = false;
    }

    // Add ORDER BY clause
    if (query->sortClause != NIL) {
        appendContextKeyword(context, " ORDER BY ",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_orderby(query->sortClause, query->targetList,
                        force_colno, context);
    }

    // Add OFFSET clause
    if (query->limitOffset != NULL) {
        appendContextKeyword(context, " OFFSET ",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        get_rule_expr(query->limitOffset, context, false);
    }

    // Add LIMIT clause (or FETCH FIRST...WITH TIES)
    if (query->limitCount != NULL) {
        if (query->limitOption == LIMIT_OPTION_WITH_TIES) {
            appendContextKeyword(context, " FETCH FIRST ",
                               -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
            appendStringInfoChar(buf, '(');
            get_rule_expr(query->limitCount, context, false);
            appendStringInfoChar(buf, ')');
            appendStringInfoString(buf, " ROWS WITH TIES");
        }
        else {
            appendContextKeyword(context, " LIMIT ",
                               -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
            if (IsA(query->limitCount, Const) &&
                ((Const *) query->limitCount)->constisnull)
                appendStringInfoString(buf, "ALL");
            else
                get_rule_expr(query->limitCount, context, false);
        }
    }

    // Add FOR UPDATE/SHARE clauses
    if (query->hasForUpdate) {
        ListCell *l;
        foreach(l, query->rowMarks) {
            RowMarkClause *rc = (RowMarkClause *) lfirst(l);

            // Skip implicit clauses
            if (rc->pushedDown)
                continue;

            // Add lock strength clause
            switch (rc->strength) {
                case LCS_FORKEYSHARE:
                    appendContextKeyword(context, " FOR KEY SHARE",
                                       -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                    break;
                case LCS_FORSHARE:
                    appendContextKeyword(context, " FOR SHARE",
                                       -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                    break;
                case LCS_FORNOKEYUPDATE:
                    appendContextKeyword(context, " FOR NO KEY UPDATE",
                                       -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                    break;
                case LCS_FORUPDATE:
                    appendContextKeyword(context, " FOR UPDATE",
                                       -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                    break;
            }

            // Add table name and wait policy
            appendStringInfo(buf, " OF %s",
                           quote_identifier(get_rtable_name(rc->rti, context)));

            if (rc->waitPolicy == LockWaitError)
                appendStringInfoString(buf, " NOWAIT");
            else if (rc->waitPolicy == LockWaitSkip)
                appendStringInfoString(buf, " SKIP LOCKED");
        }
    }
}
```