# rel_is_distinct_for

## Location
src/backend/optimizer/plan/analyzejoins.c: 861 - 957

## Overview
Determines whether a relation returns only distinct rows according to a given set of join restriction clauses, ensuring no two rows could join to the same row of another relation.

## Definition
```c
static bool rel_is_distinct_for(PlannerInfo *root, RelOptInfo *rel, List *clause_list)
```

## Detailed Description
This function performs a comprehensive analysis to determine if a relation is distinct (unique) for a specific set of join clauses. The analysis varies depending on the relation type:

**For plain relations (RTE_RELATION):**
- Uses relation_has_unique_index_for() to check if there's a unique index that covers the join columns
- The unique index analysis automatically incorporates any applicable restriction clauses

**For subqueries (RTE_SUBQUERY):**
- Extracts the relevant output column numbers and equality operators from the join clauses
- Builds argument lists for query_is_distinct_for() to analyze the subquery's distinctness
- Handles cross-type operators and RelabelType nodes appropriately
- Only considers clauses that reference actual subquery output columns

The function assumes callers have pre-validated that each clause is a mergejoinable equality with the relation's expression on one side and a non-relation expression on the other.

## Parameters / Member Variables
- `root`: Pointer to PlannerInfo structure containing planning context and relation information
- `rel`: Pointer to RelOptInfo structure representing the relation to analyze for distinctness
- `clause_list`: List of join restriction clauses (RestrictInfo nodes) that define the distinctness requirements; may be destructively modified during processing

## Dependencies
- Functions called/Symbols referenced:
  - relation_has_unique_index_for (checks unique index coverage for plain relations)
  - castNode (safely casts nodes to specific types)
  - get_rightop (extracts right operand from expressions)
  - get_leftop (extracts left operand from expressions)
  - lappend_int (appends integers to lists)
  - lappend_oid (appends OIDs to lists)
  - query_is_distinct_for (analyzes subquery distinctness)
- Called from (representative examples):
  - join_is_removable (when checking if joins can be eliminated)
  - is_innerrel_unique_for (when testing inner relation uniqueness)

## Notes and Other Information
- This is a static function within analyzejoins.c, serving as an internal utility
- The function may destructively modify the input clause_list, which is acceptable for current use cases
- Only base relations (RELOPT_BASEREL) are supported; other relation kinds return false
- The function includes redundant checks that could be skipped if all callers used rel_supports_distinctness first
- For subqueries, restriction clauses attached to the subquery itself are not currently considered (marked as XXX for potential future improvement)
- The function handles RelabelType nodes by stripping them to access the underlying Var
- Cross-type equality operators are handled appropriately by delegating complex cases to query_is_distinct_for
- The outer_is_left flag in RestrictInfo helps identify which side of the clause references the relation being analyzed
- Located in src/backend/optimizer/plan/analyzejoins.c at lines 861-957