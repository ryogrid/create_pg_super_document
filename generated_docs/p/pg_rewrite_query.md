# pg_rewrite_query

## Location
[src/backend/tcop/postgres.c:808-889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L808-L889)

## Overview
Performs query rewriting on a Query structure produced by parse analysis, transforming queries to handle views, rules, and other PostgreSQL rewrite mechanisms.

## Definition

```c
List *
pg_rewrite_query(Query *query)
```
## Detailed Description
This function is responsible for the query rewriting phase of PostgreSQL's query processing pipeline. It takes a Query structure from the parser and applies PostgreSQL's rule system to expand views, apply rules, and perform other query transformations.

The function handles two main cases:
1. **Utility Commands**: For CMD_UTILITY queries (DDL statements like CREATE, DROP, etc.), no rewriting is performed - the query is simply wrapped in a list and returned.
2. **Regular Queries**: For DML statements (SELECT, INSERT, UPDATE, DELETE), the function calls  to apply the full rewrite system.

The function includes extensive debugging support with optional checks for:
- Parse tree copying verification (COPY_PARSE_PLAN_TREES)
- Serialization/deserialization testing (WRITE_READ_PARSE_PLAN_TREES)
- Debug output for both original and rewritten parse trees

Performance statistics can be collected when  is enabled.

## Parameters / Member Variables
- `*query`: Query structure from parse analysis that needs to be rewritten
## Dependencies
- Functions called/Symbols referenced:
  - [QueryRewrite](../Q/QueryRewrite.md)
  - [elog_node_display](../e/elog_node_display.md)
  - [ResetUsage](../R/ResetUsage.md)
  - [ShowUsage](../S/ShowUsage.md)
  - copyObject
  - [equal](../e/equal.md)
  - [nodeToStringWithLocations](../n/nodeToStringWithLocations.md)
  - [stringToNodeWithLocations](../s/stringToNodeWithLocations.md)
  - list_make1
- Called from (representative examples):
  - [pg_analyze_and_rewrite_fixedparams](pg_analyze_and_rewrite_fixedparams.md)
  - [pg_analyze_and_rewrite_varparams](pg_analyze_and_rewrite_varparams.md)
  - [pg_analyze_and_rewrite_withcb](pg_analyze_and_rewrite_withcb.md)
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)
  - [inline_set_returning_function](../i/inline_set_returning_function.md)

## Notes and Other Information
- The function assumes the input query comes directly from the parser (no AcquireRewriteLocks() is performed)
- Includes comprehensive debugging infrastructure for development and testing
- Critical component in PostgreSQL's multi-phase query processing architecture
- Located in src/backend/tcop/postgres.c:808-889
- The rewrite system is essential for handling PostgreSQL's advanced features like views and rules
- Performance can be monitored via the log_parser_stats configuration parameter
- Debug output can be controlled via Debug_print_parse and Debug_print_rewritten parameters

## Simplified Source

```c
// Simplified version of pg_rewrite_query
List *pg_rewrite_query(Query *query) {
    List *querytree_list;

    // Optional debug output for parse tree
    if (Debug_print_parse)
        elog_node_display(LOG, "parse tree", query, Debug_pretty_print);

    // Optional performance monitoring
    if (log_parser_stats)
        ResetUsage();

    // Handle different query types
    if (query->commandType == CMD_UTILITY) {
        // Utility commands don't need rewriting
        querytree_list = list_make1(query);
    } else {
        // Apply rewrite rules to regular queries
        querytree_list = QueryRewrite(query);
    }

    // Show performance statistics if enabled
    if (log_parser_stats)
        ShowUsage("REWRITER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
    // Optional debugging: test copyObject() functionality
    {
        List *new_list = copyObject(querytree_list);
        if (!equal(new_list, querytree_list))
            elog(WARNING, "copyObject() failed to produce an equal rewritten parse tree");
        else
            querytree_list = new_list;
    }
#endif

#ifdef WRITE_READ_PARSE_PLAN_TREES
    // Optional debugging: test outfuncs/readfuncs serialization
    {
        List *new_list = NIL;
        ListCell *lc;

        foreach(lc, querytree_list) {
            Query *curr_query = lfirst_node(Query, lc);
            char *str = nodeToStringWithLocations(curr_query);
            Query *new_query = stringToNodeWithLocations(str);

            // Preserve queryId for pg_stat_statements
            new_query->queryId = curr_query->queryId;

            new_list = lappend(new_list, new_query);
            pfree(str);
        }

        if (!equal(new_list, querytree_list))
            elog(WARNING, "outfuncs/readfuncs failed to produce an equal rewritten parse tree");
        else
            querytree_list = new_list;
    }
#endif

    // Optional debug output for rewritten tree
    if (Debug_print_rewritten)
        elog_node_display(LOG, "rewritten parse tree", querytree_list, Debug_pretty_print);

    return querytree_list;
}
```

Key simplifications made:
- Simplified comments while preserving essential functionality
- Maintained the utility vs regular query distinction
- Preserved all debugging infrastructure with proper conditional compilation
- Kept performance monitoring capabilities
- Focused on core workflow: debug input, rewrite if needed, debug output
- Maintained the essential rewrite logic and all optional testing features