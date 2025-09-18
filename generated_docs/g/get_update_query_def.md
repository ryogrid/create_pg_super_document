# get_update_query_def

## Location
[src/backend/utils/adt/ruleutils.c:6863-6918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L6863-L6918)

## Overview
Generates the text representation of an UPDATE SQL statement from a parsed Query structure, reconstructing the complete UPDATE command with all its clauses.

## Definition
```c
static void get_update_query_def(Query *query, deparse_context *context)
```

## Detailed Description
This function is responsible for deparsing (converting back to text) an UPDATE query from PostgreSQL's internal Query representation. It reconstructs the complete UPDATE statement including the WITH clause (if present), the target relation, SET clause with target list, FROM clause, WHERE conditions, and RETURNING clause. The function formats the output according to pretty-printing preferences specified in the deparse context.

The function follows PostgreSQL's standard deparsing pattern:
1. Handles WITH clause for CTEs
2. Generates the UPDATE relation_name portion with proper aliasing
3. Processes the SET clause by calling specialized targetlist deparsing
4. Adds FROM clause for multi-table updates
5. Includes WHERE clause for filtering
6. Appends RETURNING clause if specified

## Parameters / Member Variables
- `query`: The Query structure containing the parsed UPDATE statement to be deparsed
- `context`: The deparse_context containing formatting preferences, indentation level, and the output StringInfo buffer

## Dependencies
- Functions called/Symbols referenced:
  - [get_with_clause](get_with_clause.md)
  - rt_fetch  
  - only_marker
  - generate_relation_name
  - get_rte_alias
  - [get_update_query_targetlist_def](get_update_query_targetlist_def.md)
  - get_from_clause
  - appendContextKeyword
  - get_rule_expr
  - [get_target_list](get_target_list.md)
- Called from:
  - [get_query_def](get_query_def.md)

## Notes and Other Information
- This is a static function within ruleutils.c, part of PostgreSQL's rule decompilation system
- The function assumes the query is a valid UPDATE query (resultRelation points to a valid RTE_RELATION)
- Pretty-printing behavior is controlled through PRETTY_INDENT context settings
- The function handles complex UPDATE scenarios including those with FROM clauses for multi-table updates
- Part of the broader query deparsing infrastructure used for rule definitions, view definitions, and query display