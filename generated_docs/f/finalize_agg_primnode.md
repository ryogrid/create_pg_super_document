# finalize_agg_primnode

## Location
src/backend/optimizer/plan/subselect.c: 2974 - 3000

## Overview
Specialized function that finds all Aggref nodes in an expression tree and collects PARAM_EXEC parameter IDs from their aggregated arguments and filters.

## Definition
```c
static bool finalize_agg_primnode(Node *node, finalize_primnode_context *context)
```

## Detailed Description
finalize_agg_primnode is a specialized variant of finalize_primnode designed specifically for processing aggregate function references in the context of hashed aggregation. This function is used when analyzing AGG_HASHED plans to determine which parameters are referenced within aggregate function calls.

The function traverses expression trees looking specifically for Aggref nodes (aggregate function references). When it finds an Aggref node, it:

1. **Processes aggregate arguments**: Uses finalize_primnode to analyze the arguments list (agg->args) to collect any PARAM_EXEC parameters used within the aggregated expressions
2. **Processes aggregate filters**: Similarly processes the aggregate filter expression (agg->aggfilter) if present
3. **Stops recursion**: Returns false to prevent further traversal below Aggref nodes, since there cannot be nested aggregates

For non-Aggref nodes, the function continues standard tree traversal using expression_tree_walker.

This specialized processing is necessary for hashed aggregation plans because the optimizer needs to know which parameters are used within aggregate computations to properly handle parameter changes and determine when aggregate hash tables need to be rebuilt.

## Parameters / Member Variables
- `node`: The expression node to be processed (can be NULL)
- `context`: finalize_primnode_context structure containing:
  - root: PlannerInfo for accessing global planning information  
  - paramids: Bitmapset that accumulates discovered parameter IDs from aggregate arguments

## Dependencies
- Functions called/Symbols referenced:
  - finalize_primnode (processes aggregate arguments and filters)
  - expression_tree_walker (general expression traversal)
  - Node type checking macros (IsA)
- Called from (representative examples):
  - finalize_plan (specifically for AGG_HASHED plans in Agg node processing)
  - finalize_agg_primnode (recursive calls)

## Notes and Other Information
- Returns false to continue tree walking (standard expression_tree_walker protocol)
- Used exclusively for analyzing hashed aggregation plans (AGG_HASHED strategy)
- Does not process direct arguments of Aggref nodes - only the aggregated expressions within args and aggfilter
- The comment mentions not considering "direct arguments" which refers to non-aggregated arguments that might exist in some aggregate function contexts
- Critical for determining when hash tables need rebuilding in response to parameter changes
- Part of PostgreSQL's parameter finalization subsystem specialized for aggregate processing
- Located in src/backend/optimizer/plan/subselect.c (static function)