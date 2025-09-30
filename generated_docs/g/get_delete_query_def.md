# get_delete_query_def

## Location
[src/backend/utils/adt/ruleutils.c:7071-7121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7071-L7121)

## Overview
Generates the text representation of a DELETE SQL statement from a parsed Query structure, reconstructing the complete DELETE command with all its clauses.

## Definition
```c
static void get_delete_query_def(Query *query, deparse_context *context)
```

## Detailed Description
This function is responsible for deparsing (converting back to text) a DELETE query from PostgreSQL's internal Query representation. It reconstructs the complete DELETE statement following the standard DELETE syntax pattern including the WITH clause (if present), the target relation, USING clause for multi-table deletes, WHERE conditions, and RETURNING clause.

The function follows PostgreSQL's standard deparsing pattern:
1. Handles WITH clause for Common Table Expressions (CTEs)
2. Generates the DELETE FROM relation_name portion with proper aliasing
3. Adds USING clause for multi-table delete operations (equivalent to FROM in UPDATE)
4. Includes WHERE clause for row filtering
5. Appends RETURNING clause if specified

The output formatting respects pretty-printing preferences specified in the deparse context, including proper indentation.

## Parameters / Member Variables
- `query`: The Query structure containing the parsed DELETE statement to be deparsed
- `context`: The deparse_context containing formatting preferences, indentation level, and the output StringInfo buffer

## Dependencies
- Functions called/Symbols referenced:
  - [get_with_clause](get_with_clause.md)
  - rt_fetch
  - only_marker
  - [generate_relation_name](generate_relation_name.md)
  - [get_rte_alias](get_rte_alias.md)
  - [get_from_clause](get_from_clause.md)
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [get_rule_expr](get_rule_expr.md)
  - [get_target_list](get_target_list.md)
- Called from:
  - [get_query_def](get_query_def.md)

## Notes and Other Information
- This is a static function within ruleutils.c, part of PostgreSQL's rule decompilation system
- The function assumes the query is a valid DELETE query (resultRelation points to a valid RTE_RELATION)
- Pretty-printing behavior is controlled through PRETTY_INDENT context settings and PRETTYINDENT_STD constants
- The USING clause in DELETE serves the same purpose as the FROM clause in UPDATE - it allows additional tables to be referenced for filtering
- Part of the broader query deparsing infrastructure used for rule definitions, view definitions, and query display
- The function handles both simple single-table deletes and complex multi-table delete scenarios
- RETURNING clause support allows the function to handle DELETE statements that return data from deleted rows

## Simplified Source
```c
static void get_delete_query_def(Query *query, deparse_context *context) {
    StringInfo buf = context->buf;
    RangeTblEntry *rte;

    // Add WITH clause for CTEs if present
    get_with_clause(query, context);

    // Generate DELETE FROM relation_name
    rte = rt_fetch(query->resultRelation, query->rtable);

    if (PRETTY_INDENT(context)) {
        appendStringInfoChar(buf, ' ');
        context->indentLevel += PRETTYINDENT_STD;
    }

    appendStringInfo(buf, "DELETE FROM %s%s",
                     only_marker(rte),
                     generate_relation_name(rte->relid, NIL));

    // Add relation alias if needed
    get_rte_alias(rte, query->resultRelation, false, context);

    // Add USING clause for multi-table deletes
    get_from_clause(query, " USING ", context);

    // Add WHERE clause if present
    if (query->jointree->quals != NULL) {
        appendContextKeyword(context, " WHERE ",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_expr(query->jointree->quals, context, false);
    }

    // Add RETURNING clause if present
    if (query->returningList) {
        appendContextKeyword(context, " RETURNING",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_target_list(query->returningList, context);
    }
}
```