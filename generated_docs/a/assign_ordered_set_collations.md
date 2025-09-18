# assign_ordered_set_collations

## Location
src/backend/parser/parse_collate.c: 919 - 954

## Overview
Handles collation assignment for ordered-set aggregate functions, using a sophisticated approach to balance collation determination between direct arguments and aggregated sort columns.

## Definition


## Detailed Description
This function implements collation assignment for ordered-set aggregates (AGGKIND_ORDERED_SET), which have both direct arguments and aggregated arguments that define sort ordering. The challenge is determining when aggregated sort arguments should contribute to the aggregate's result collation versus being treated independently.

The function uses a nuanced approach:
1. **Direct arguments**: Always contribute normally to the aggregate's collation via 
2. **Aggregated arguments**: Treatment depends on aggregate signature:
   - **Single non-variadic**: Aggregated argument contributes to result collation (allows collation to flow through)
   - **Multiple or variadic**: Aggregated arguments processed independently as sort columns to avoid conflicts

This prevents errors in cases like  while ensuring that single-argument ordered-set aggregates can properly inherit collation from their sort argument.

## Parameters / Member Variables
- : Pointer to the Aggref node representing the ordered-set aggregate function call
- : Local collation context for accumulating collation state from direct and qualifying aggregated arguments

## Dependencies
- Functions called/Symbols referenced:
  -  (for direct arguments and single aggregated arguments)
  -  (for multiple aggregated arguments treated as independent sort columns)
  -  (to check if function is variadic)
  -  (to count aggregated arguments)
- Called from (representative examples):
  -  (when processing AGGKIND_ORDERED_SET aggregates)

## Notes and Other Information
- The merge_sort_collations flag determines whether aggregated arguments contribute to the aggregate's collation
- The decision is based on having exactly one aggregated argument AND the function being non-variadic
- This approach ensures backward compatibility while supporting complex ordered-set aggregates with multiple sort columns
- Examples of ordered-set aggregates include percentile_cont, percentile_disc, and mode functions