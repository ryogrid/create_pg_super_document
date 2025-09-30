# get_insert_query_def

## Location
[src/backend/utils/adt/ruleutils.c:6647-6862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L6647-L6862)

## Overview
Converts an internal INSERT Query structure into its textual SQL representation for rule deparsing, handling all INSERT variants including VALUES, SELECT, and ON CONFLICT clauses.

## Definition
```c
static void get_insert_query_def(Query *query, deparse_context *context)
```

## Detailed Description
This comprehensive function reconstructs INSERT statements from their internal parse tree representation. It handles the full spectrum of PostgreSQL INSERT syntax:

**Core INSERT components:**
- WITH clauses for common table expressions
- Target relation name with proper aliasing
- Column name lists with indirection (array subscripts, field access)
- OVERRIDING SYSTEM/USER VALUE clauses

**Data source variants:**
- Single-row VALUES with expression list
- Multi-row VALUES from VALUES RTEs  
- INSERT ... SELECT from subquery RTEs
- DEFAULT VALUES when no data is specified

**Advanced features:**
- ON CONFLICT clauses with arbiter specifications
- ON CONFLICT DO NOTHING/UPDATE actions
- Constraint-based and index-based conflict detection
- WHERE clauses for partial unique indexes
- RETURNING clauses for output

The function intelligently determines the INSERT type by examining the range table entries (RTEs) and reconstructs the appropriate syntax while preserving all semantic information.

## Parameters / Member Variables
- `query`: Query structure containing the complete INSERT parse tree
- `context`: Deparse context with output buffer, indentation, and formatting state

## Dependencies
- Functions called/Symbols referenced:
  - [get_with_clause](get_with_clause.md) (for WITH clause processing)
  - [generate_relation_name](generate_relation_name.md) (for target table name)
  - [get_rte_alias](get_rte_alias.md) (for table aliasing)
  - [get_attname](get_attname.md)/quote_identifier (for column names)
  - [processIndirection](../p/processIndirection.md) (for complex column references)
  - [get_query_def](get_query_def.md) (for INSERT ... SELECT subqueries)
  - [get_values_def](get_values_def.md) (for multi-row VALUES)
  - [get_rule_expr](get_rule_expr.md) (for expressions and conflict specifications)
  - [get_target_list](get_target_list.md) (for RETURNING clauses)
- Called from (representative examples):
  - [get_query_def](get_query_def.md) (main query deparsing entry point)

## Notes and Other Information
- Static function accessible only within ruleutils.c
- Handles PostgreSQL's INSERT extensions like ON CONFLICT ("UPSERT" functionality)
- Properly manages indentation and formatting through the context system
- Critical for view definition reconstruction and rule display
- Located at src/backend/utils/adt/ruleutils.c:6647-6862
- One of the largest and most complex query reconstruction functions due to INSERT's syntactic variety

## Simplified Source
```c
static void get_insert_query_def(Query *query, deparse_context *context) {
    StringInfo buf = context->buf;
    RangeTblEntry *select_rte = NULL;
    RangeTblEntry *values_rte = NULL;
    RangeTblEntry *rte;
    List *strippedexprs = NIL;

    // Add WITH clause if present
    get_with_clause(query, context);

    // Find the type of INSERT (SELECT, VALUES, or simple)
    ListCell *l;
    foreach(l, query->rtable) {
        rte = (RangeTblEntry *) lfirst(l);
        if (rte->rtekind == RTE_SUBQUERY)
            select_rte = rte;
        else if (rte->rtekind == RTE_VALUES)
            values_rte = rte;
    }

    // Start with INSERT INTO relation_name
    rte = rt_fetch(query->resultRelation, query->rtable);

    if (PRETTY_INDENT(context)) {
        context->indentLevel += PRETTYINDENT_STD;
        appendStringInfoChar(buf, ' ');
    }

    appendStringInfo(buf, "INSERT INTO %s",
                     generate_relation_name(rte->relid, NIL));

    // Add relation alias if needed
    get_rte_alias(rte, query->resultRelation, true, context);
    appendStringInfoChar(buf, ' ');

    // Add column list
    char *sep = "";
    if (query->targetList)
        appendStringInfoChar(buf, '(');

    foreach(l, query->targetList) {
        TargetEntry *tle = (TargetEntry *) lfirst(l);
        if (tle->resjunk)
            continue;

        appendStringInfoString(buf, sep);
        sep = ", ";

        // Add column name from catalog
        appendStringInfoString(buf,
            quote_identifier(get_attname(rte->relid, tle->resno, false)));

        // Process any indirection and collect expressions
        strippedexprs = lappend(strippedexprs,
                              processIndirection((Node *) tle->expr, context));
    }

    if (query->targetList)
        appendStringInfoString(buf, ") ");

    // Add OVERRIDING clause if present
    if (query->override) {
        if (query->override == OVERRIDING_SYSTEM_VALUE)
            appendStringInfoString(buf, "OVERRIDING SYSTEM VALUE ");
        else if (query->override == OVERRIDING_USER_VALUE)
            appendStringInfoString(buf, "OVERRIDING USER VALUE ");
    }

    // Add the data source
    if (select_rte) {
        // INSERT ... SELECT
        get_query_def(select_rte->subquery, buf, context->namespaces, NULL,
                      false, context->prettyFlags, context->wrapColumn,
                      context->indentLevel);
    }
    else if (values_rte) {
        // Multi-row VALUES
        get_values_def(values_rte->values_lists, context);
    }
    else if (strippedexprs) {
        // Single VALUES
        appendContextKeyword(context, "VALUES (",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
        get_rule_list_toplevel(strippedexprs, context, false);
        appendStringInfoChar(buf, ')');
    }
    else {
        // DEFAULT VALUES
        appendStringInfoString(buf, "DEFAULT VALUES");
    }

    // Add ON CONFLICT clause (simplified)
    if (query->onConflict) {
        OnConflictExpr *confl = query->onConflict;
        appendStringInfoString(buf, " ON CONFLICT");

        // Add conflict target (index columns or constraint)
        if (confl->arbiterElems) {
            appendStringInfoChar(buf, '(');
            get_rule_expr((Node *) confl->arbiterElems, context, false);
            appendStringInfoChar(buf, ')');

            if (confl->arbiterWhere != NULL) {
                appendContextKeyword(context, " WHERE ",
                                   -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
                get_rule_expr(confl->arbiterWhere, context, false);
            }
        }
        else if (OidIsValid(confl->constraint)) {
            char *constraint = get_constraint_name(confl->constraint);
            appendStringInfo(buf, " ON CONSTRAINT %s",
                           quote_identifier(constraint));
        }

        // Add conflict action
        if (confl->action == ONCONFLICT_NOTHING) {
            appendStringInfoString(buf, " DO NOTHING");
        }
        else {
            appendStringInfoString(buf, " DO UPDATE SET ");
            get_update_query_targetlist_def(query, confl->onConflictSet,
                                          context, rte);

            if (confl->onConflictWhere != NULL) {
                appendContextKeyword(context, " WHERE ",
                                   -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
                get_rule_expr(confl->onConflictWhere, context, false);
            }
        }
    }

    // Add RETURNING clause if present
    if (query->returningList) {
        appendContextKeyword(context, " RETURNING",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_target_list(query->returningList, context);
    }
}
```