# assign_hypothetical_collations

## Location
src/backend/parser/parse_collate.c: 955 - 1058

## Overview
Handles collation assignment for hypothetical-set aggregates by unifying collations between paired hypothetical and aggregated arguments according to SQL standard requirements.

## Definition


## Detailed Description
This function implements the most complex collation assignment logic for hypothetical-set aggregates (AGGKIND_HYPOTHETICAL). These aggregates require special handling because:

1. **Paired Arguments**: Hypothetical direct arguments must be unified with their corresponding aggregated arguments (e.g., in , val and col must have compatible collations)
2. **Forced Collation**: The chosen collation must be propagated down to the sort column to ensure proper sorting behavior
3. **Conditional Contribution**: Direct arguments contribute to aggregate collation only when their partner aggregated arguments do

The function processes arguments in three phases:
1. **Extra Direct Args**: Non-hypothetical direct arguments processed normally
2. **Paired Processing**: Each hypothetical/aggregated pair is unified using a local context, conflicts are immediately reported
3. **Collation Propagation**: If needed, a RelabelType node is inserted to force the unified collation on the sort column

## Parameters / Member Variables  
- : Pointer to the Aggref node representing the hypothetical-set aggregate function call
- : Local collation context for accumulating collation state from qualifying argument pairs

## Dependencies
- Functions called/Symbols referenced:
  -  (for processing individual arguments)
  -  (for combining pair collation with aggregate collation)
  -  (for forcing collation on sort columns)
  - ,  (to determine merge behavior)
  - , ,  (expression introspection)
  -  (for error messages)
- Called from (representative examples):
  -  (when processing AGGKIND_HYPOTHETICAL aggregates)

## Notes and Other Information
- The merge_sort_collations flag works similarly to ordered-set aggregates (single non-variadic argument)
- RelabelType injection is noted as "grotty" but necessary for proper collation enforcement during sorting
- The RelabelType approach avoids changing implicit collations to explicit ones during dump/reload
- Examples of hypothetical-set aggregates include rank, dense_rank, percent_rank, and cume_dist functions
- Collation conflicts between paired arguments are reported immediately rather than deferred