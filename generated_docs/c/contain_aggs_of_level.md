# contain_aggs_of_level

## Location
[src/backend/rewrite/rewriteManip.c:86-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L86-L102)

## Overview
Checks if an expression contains an aggregate function call of a specified query level, distinguishing between aggregates belonging to the target level versus those in subqueries or outer queries.

## Definition
```c
bool contain_aggs_of_level(Node *node, int levelsup)
```

## Detailed Description
This function is part of PostgreSQL's rewrite system and serves a crucial role in query analysis by detecting aggregate functions at specific query nesting levels. The function uses a tree walking mechanism to traverse expression trees and identify aggregate functions that logically belong to the specified query level.

The function is designed to handle complex query structures with nested subqueries, ensuring that only aggregates at the target level are detected while ignoring aggregates in subqueries or outer queries. It can operate on both Query nodes and bare expression trees, automatically handling the different starting contexts.

## Parameters / Member Variables
- `node`: The root node of the expression tree or Query structure to examine
- `levelsup`: The target query level to search for aggregates (0 for current level, positive values for outer levels)

## Dependencies
- Functions called/Symbols referenced:
  - [contain_aggs_of_level_context](contain_aggs_of_level_context.md) (context structure)
  - query_or_expression_tree_walker (tree traversal utility)
  - [contain_aggs_of_level_walker](contain_aggs_of_level_walker.md) (callback function for tree walking)
- Called from (representative examples):
  - [convert_EXISTS_to_ANY](convert_EXISTS_to_ANY.md) (subselect optimization)
  - [checkTargetlistEntrySQL92](checkTargetlistEntrySQL92.md) (SQL standard compliance checking)
  - [AddQual](../A/AddQual.md) (query rewriting)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:86-102
- Part of PostgreSQL's query rewrite infrastructure
- Uses the query_or_expression_tree_walker pattern for robust tree traversal
- Critical for maintaining proper aggregate function semantics in complex nested queries
- The function must be prepared to handle both Query nodes and bare expression trees as starting points