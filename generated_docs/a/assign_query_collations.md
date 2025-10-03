# assign_query_collations

## Location
[src/backend/parser/parse_collate.c:101-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_collate.c#L101-L125)

## Overview
Marks all expressions in a given Query with collation information after completion of parse analysis.

## Definition

```c
void
assign_query_collations(ParseState *pstate, Query *query)
```
## Detailed Description
This function serves as the main entry point for assigning collation information to all expressions within a parsed Query structure. It utilizes query_tree_walker() to traverse the query tree and apply collation assignment to contained expressions. The function specifically avoids recursing into sub-Queries since those should have been processed when they were built. It also skips the range table and CTE subqueries, as RTEs and subqueries must have been processed already to ensure that Vars referring to them are created with the correct collation.

## Parameters / Member Variables
- `*pstate`: ParseState context containing parsing state information
- `*query`: The Query structure whose expressions need collation assignment
## Dependencies
- Functions called/Symbols referenced:
  - query_tree_walker
  - [assign_query_collations_walker](assign_query_collations_walker.md)
  - [QTW_IGNORE_RANGE_TABLE](../Q/QTW_IGNORE_RANGE_TABLE.md)
  - [QTW_IGNORE_CTE_SUBQUERIES](../Q/QTW_IGNORE_CTE_SUBQUERIES.md)
- Called from (representative examples):
  - [transformDeleteStmt](../t/transformDeleteStmt.md) (src/backend/parser/analyze.c:566)
  - [transformInsertStmt](../t/transformInsertStmt.md) (src/backend/parser/analyze.c:992)
  - [transformSelectStmt](../t/transformSelectStmt.md) (src/backend/parser/analyze.c:1463)
  - [transformValuesClause](../t/transformValuesClause.md) (src/backend/parser/analyze.c:1683)
  - [transformSetOperationStmt](../t/transformSetOperationStmt.md) (src/backend/parser/analyze.c:1940)
  - [transformReturnStmt](../t/transformReturnStmt.md) (src/backend/parser/analyze.c:2408)
  - [transformUpdateStmt](../t/transformUpdateStmt.md) (src/backend/parser/analyze.c:2475)
  - [transformMergeStmt](../t/transformMergeStmt.md) (src/backend/parser/parse_merge.c:409)

## Notes and Other Information
This function should be applied to each Query after completion of parse analysis for expressions. It deliberately ignores range tables and CTE subqueries during traversal, assuming they have been properly processed during their creation phase. The function is defined in src/backend/parser/parse_collate.c at lines 101-125.

## Simplified Source

```c
void assign_query_collations(ParseState *pstate, Query *query) {
    // Walk the query tree to assign collations to all expressions
    query_tree_walker(query,
                      assign_query_collations_walker,
                      (void *) pstate,
                      QTW_IGNORE_RANGE_TABLE | QTW_IGNORE_CTE_SUBQUERIES);
}
```

**Key Points:**
- Simple wrapper that delegates to tree walking mechanism
- Assigns collation information to all expressions in a parsed Query
- Ignores range tables and CTE subqueries (already processed when built)
- Uses specialized walker function for actual collation assignment logic
- Called after parse analysis completion for each Query