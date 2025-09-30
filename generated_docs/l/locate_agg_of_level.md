# locate_agg_of_level

## Location
[src/backend/rewrite/rewriteManip.c:150-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L150-L169)

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
  - [locate_agg_of_level_context](locate_agg_of_level_context.md) (context structure)
  - query_or_expression_tree_walker (tree traversal utility)
  - [locate_agg_of_level_walker](locate_agg_of_level_walker.md) (callback function for tree walking)
- Called from (representative examples):
  - [check_agg_arguments](../c/check_agg_arguments.md) (aggregate validation)
  - [parseCheckAggregates](../p/parseCheckAggregates.md) (parser aggregate checking)
  - [checkTargetlistEntrySQL92](../c/checkTargetlistEntrySQL92.md) (SQL standard compliance)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:150-169
- Returns -1 if no aggregate found or parse location unknown
- Designed specifically for error reporting rather than performance-critical operations
- Uses the same tree walking pattern as contain_aggs_of_level but with different context and return semantics
- The function deliberately maintains a separate API from contain_aggs_of_level for clarity
- Critical for providing meaningful error messages with precise source locations

## Simplified Source

```c
int locate_agg_of_level(Node *node, int levelsup) {
    locate_agg_of_level_context context;

    // Initialize context for tree walking
    context.agg_location = -1;        // Default: no aggregate found
    context.sublevels_up = levelsup;  // Target query nesting level

    // Walk the expression/query tree to find aggregates at specified level
    // Returns ignored since we only care about the location stored in context
    (void) query_or_expression_tree_walker(node,
                                          locate_agg_of_level_walker,
                                          (void *) &context,
                                          0);

    // Return parse location of found aggregate, or -1 if none found
    return context.agg_location;
}
```