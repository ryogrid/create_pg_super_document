# contain_aggs_of_level_context

## Location
[src/backend/rewrite/rewriteManip.c:31-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L31-L36)

## Overview
A context structure used to track the target query level when searching for aggregate functions in an expression tree.

## Definition


## Detailed Description
This structure serves as a context parameter for the tree walker functions that detect aggregate functions at a specific query level. The structure maintains the current sublevel depth during recursive traversal of expression trees, allowing the walker to identify aggregates that belong to a particular nesting level in queries with subqueries.

The context is used in conjunction with  function to determine whether an expression contains aggregate function calls at a specified query level. This is crucial for query rewriting operations where it's necessary to distinguish between aggregates belonging to the current query level versus those in subqueries or outer queries.

## Parameters / Member Variables
- : The target query level depth to search for aggregates; represents how many levels up from the current context to look for matching aggregates

## Dependencies
- Functions called/Symbols referenced: None (pure data structure)
- Called from (representative examples):
  - [contain_aggs_of_level](contain_aggs_of_level.md) (src/backend/rewrite/rewriteManip.c:88)
  - [contain_aggs_of_level_walker](contain_aggs_of_level_walker.md) (src/backend/rewrite/rewriteManip.c:104)

## Notes and Other Information
- Part of PostgreSQL's query rewriting infrastructure in rewriteManip.c
- Used specifically for aggregate function detection during query transformation
- The structure is intentionally simple, containing only the level tracking information needed for the tree traversal
- Works in conjunction with expression_tree_walker and query_tree_walker functions