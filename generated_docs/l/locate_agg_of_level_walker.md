# locate_agg_of_level_walker

## Location
[src/backend/rewrite/rewriteManip.c:170-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L170-L215)

## Overview
A recursive tree walker callback function that searches for aggregate functions at a specific query level and captures their parse locations for error reporting.

## Definition
```c
static bool locate_agg_of_level_walker(Node *node, locate_agg_of_level_context *context)
```

## Detailed Description
This static function serves as the core implementation for locate_agg_of_level functionality, operating as a specialized tree walker callback that not only detects aggregates at specific levels but also captures their parse locations. It's designed specifically for error reporting scenarios where precise source location information is needed.

The function handles multiple node types with location-aware logic:
- For Aggref nodes: Checks level match AND validates that location is non-negative (>= 0)
- For GroupingFunc nodes: Similar level and location checking as Aggref
- For Query nodes: Recursively descends with proper level adjustment
- For other nodes: Continues traversal using expression_tree_walker

The key difference from contain_aggs_of_level_walker is the additional location validation and storage. The function stores the first valid location found and immediately terminates traversal, making it efficient for error reporting purposes.

## Parameters / Member Variables
- `node`: The current node being examined in the tree traversal
- `context`: Context structure containing target sublevels_up and storage for agg_location

## Dependencies
- Functions called/Symbols referenced:
  - [locate_agg_of_level_context](locate_agg_of_level_context.md) (context structure)
  - [Aggref](../A/Aggref.md) (aggregate function reference node)
  - [GroupingFunc](../G/GroupingFunc.md) (GROUPING function node) 
  - query_tree_walker (subquery traversal)
  - expression_tree_walker (general expression traversal)
- Called from (representative examples):
  - [locate_agg_of_level](locate_agg_of_level.md) (main entry point)
  - [locate_agg_of_level_walker](locate_agg_of_level_walker.md) (recursive self-calls)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:170-215
- Static function, only accessible within the same compilation unit
- Returns true immediately upon finding a matching aggregate with valid location (short-circuit)
- Validates that parse location is non-negative before storing it
- Critical for providing precise error messages with source location information
- Handles proper level adjustment when recursing into subqueries
- Optimized for first-match scenarios typical in error reporting

## Simplified Source

```c
static bool
locate_agg_of_level_walker(Node *node, locate_agg_of_level_context *context)
{
    if (node == NULL)
        return false;

    // Check for aggregate function at target level
    if (IsA(node, Aggref)) {
        Aggref *aggref = (Aggref *) node;
        if (aggref->agglevelsup == context->sublevels_up && aggref->location >= 0) {
            context->agg_location = aggref->location;
            return true;  // Found target aggregate, stop traversal
        }
        // Continue to examine aggregate arguments
    }

    // Check for GROUPING function at target level
    if (IsA(node, GroupingFunc)) {
        GroupingFunc *groupfunc = (GroupingFunc *) node;
        if (groupfunc->agglevelsup == context->sublevels_up && groupfunc->location >= 0) {
            context->agg_location = groupfunc->location;
            return true;  // Found target grouping function, stop traversal
        }
    }

    // Handle subqueries with level adjustment
    if (IsA(node, Query)) {
        bool result;
        context->sublevels_up++;
        result = query_tree_walker((Query *) node, locate_agg_of_level_walker, context, 0);
        context->sublevels_up--;
        return result;
    }

    // Continue traversal for other node types
    return expression_tree_walker(node, locate_agg_of_level_walker, context);
}
```