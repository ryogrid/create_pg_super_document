# get_query_def

## Location
[src/backend/utils/adt/ruleutils.c:5437-5519](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5437-L5519)

## Overview
Converts a Query parse tree back into readable SQL text by dispatching to appropriate command-specific formatting functions based on the query's command type.

## Definition

```c
static void
get_query_def(Query *query, StringInfo buf, List *parentnamespace,
			  TupleDesc resultDesc, bool colNamesVisible,
			  int prettyFlags, int wrapColumn, int startIndent)
```
## Detailed Description
The  function serves as the central dispatcher for converting PostgreSQL's internal Query parse trees back into human-readable SQL text. It sets up the deparse context with formatting parameters and namespace information, then routes the query to the appropriate specialized function based on its command type (SELECT, INSERT, UPDATE, DELETE, MERGE, UTILITY, or NOTHING).

Before deparsing begins, the function performs important setup tasks:
- Guards against stack overflow and interrupts for deeply nested or long-running operations
- Acquires necessary locks on referenced relations using AccessShareLock (read-only)
- Initializes a deparse_context structure with all formatting parameters
- Sets up namespace resolution for handling table and column references

The function uses a switch statement to dispatch to command-specific handlers like , , etc., ensuring that each SQL command type is formatted according to its specific syntax requirements.

## Parameters / Member Variables
- `*query`: Query parse tree to be converted back to SQL text
- `buf`: StringInfo buffer where the generated SQL text will be appended
- `*parentnamespace`: List of outer-level deparse_namespace structures for nested query context
- `resultDesc`: Optional tuple descriptor for SELECT queries, used to provide preferred column names for output
- `colNamesVisible`: Boolean indicating whether column names should be visible in the current context
- `prettyFlags`: Bitmask of PRETTYFLAG_XXX options controlling formatting style
- `wrapColumn`: Maximum line length for wrapping, or -1 to disable line wrapping
- `startIndent`: Initial indentation level for the generated SQL
## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [check_stack_depth](../c/check_stack_depth.md)
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md)
  - [lcons](../l/lcons.md)
  - [list_copy](../l/list_copy.md)
  - [set_deparse_for_query](../s/set_deparse_for_query.md)
  - [get_select_query_def](get_select_query_def.md)
  - [get_update_query_def](get_update_query_def.md)
  - [get_insert_query_def](get_insert_query_def.md)
  - [get_delete_query_def](get_delete_query_def.md)
  - [get_merge_query_def](get_merge_query_def.md)
  - [get_utility_query_def](get_utility_query_def.md)
  - CMD_SELECT, CMD_UPDATE, CMD_INSERT, CMD_DELETE, CMD_MERGE, CMD_NOTHING, CMD_UTILITY
- Called from (representative examples):
  - [pg_get_querydef](../p/pg_get_querydef.md)
  - [make_ruledef](../m/make_ruledef.md)
  - [make_viewdef](../m/make_viewdef.md)
  - [get_with_clause](get_with_clause.md)
  - [get_setop_query](get_setop_query.md)
  - [get_insert_query_def](get_insert_query_def.md)
  - [get_sublink_expr](get_sublink_expr.md)
  - [get_from_clause_item](get_from_clause_item.md)

## Notes and Other Information
This function is a core component of PostgreSQL's rule system and query deparsing infrastructure. It's used extensively throughout the system for generating readable SQL from internal parse trees, particularly in view definitions, rule definitions, and query introspection. The function is designed to handle nested queries and maintains proper namespace resolution through the parentnamespace parameter. The locking mechanism ensures consistency when deparsing queries that reference database objects that might be modified concurrently.

## Simplified Source

```c
static void get_query_def(Query *query, StringInfo buf, List *parentnamespace,
                         TupleDesc resultDesc, bool colNamesVisible,
                         int prettyFlags, int wrapColumn, int startIndent) {
    deparse_context context;
    deparse_namespace dpns;

    // Safety checks for long/deeply-nested queries
    CHECK_FOR_INTERRUPTS();
    check_stack_depth();

    // Acquire locks on referenced relations for consistency
    AcquireRewriteLocks(query, false, false);

    // Initialize deparse context
    context.buf = buf;
    context.namespaces = lcons(&dpns, list_copy(parentnamespace));
    context.resultDesc = NULL;
    context.targetList = NIL;
    context.windowClause = NIL;
    context.varprefix = (parentnamespace != NIL || list_length(query->rtable) != 1);
    context.prettyFlags = prettyFlags;
    context.wrapColumn = wrapColumn;
    context.indentLevel = startIndent;
    context.colNamesVisible = colNamesVisible;
    context.inGroupBy = false;
    context.varInOrderBy = false;
    context.appendparents = NULL;

    // Set up namespace for this query
    set_deparse_for_query(&dpns, query, parentnamespace);

    // Dispatch to appropriate command handler
    switch (query->commandType) {
        case CMD_SELECT:
            context.resultDesc = resultDesc;
            get_select_query_def(query, &context);
            break;
        case CMD_UPDATE:
            get_update_query_def(query, &context);
            break;
        case CMD_INSERT:
            get_insert_query_def(query, &context);
            break;
        case CMD_DELETE:
            get_delete_query_def(query, &context);
            break;
        case CMD_MERGE:
            get_merge_query_def(query, &context);
            break;
        case CMD_NOTHING:
            appendStringInfoString(buf, "NOTHING");
            break;
        case CMD_UTILITY:
            get_utility_query_def(query, &context);
            break;
        default:
            elog(ERROR, "unrecognized query command type: %d", query->commandType);
            break;
    }
}
```