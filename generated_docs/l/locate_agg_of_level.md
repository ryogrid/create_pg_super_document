# locate_agg_of_level

## Location
src/backend/rewrite/rewriteManip.c: 150 - 169

## Overview
Finds the parse location of any aggregate function at the specified query level, primarily used for error reporting and diagnostic purposes.

## Definition
```c
int locate_agg_of_level(Node *node, int levelsup)
```

## Detailed Description
This function serves a specialized role in PostgreSQL's parser and error reporting system by locating the parse position of aggregate functions at specific query nesting levels. Unlike contain_aggs_of_level which only determines presence, this function identifies the exact source location where an aggregate appears.

The function is specifically designed for error reporting scenarios where PostgreSQL needs to provide precise location information to users about problematic aggregate usage. It maintains a separate API from contain_aggs_of_level to keep both functions focused on their specific purposes without complicating their interfaces.

The function returns the parse location as an integer offset, or -1 if no matching aggregate is found or if all matching aggregates have unknown parse locations. This makes it suitable for diagnostic purposes where location information enhances error messages.

## Parameters / Member Variables
- `node`: The root node of the expression tree or Query structure to examine
- `levelsup`: The target query level to search for aggregates (0 for current level, positive values for outer levels)

## Dependencies
- Functions called/Symbols referenced:
  - locate_agg_of_level_context (context structure)
  - query_or_expression_tree_walker (tree traversal utility)
  - locate_agg_of_level_walker (callback function for tree walking)
- Called from (representative examples):
  - check_agg_arguments (aggregate validation)
  - parseCheckAggregates (parser aggregate checking)
  - checkTargetlistEntrySQL92 (SQL standard compliance)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:150-169
- Returns -1 if no aggregate found or parse location unknown
- Designed specifically for error reporting rather than performance-critical operations
- Uses the same tree walking pattern as contain_aggs_of_level but with different context and return semantics
- The function deliberately maintains a separate API from contain_aggs_of_level for clarity
- Critical for providing meaningful error messages with precise source locations