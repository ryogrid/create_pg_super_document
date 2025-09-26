# choose_hashed_setop

## Location
[src/backend/optimizer/prep/prepunion.c:1290-1396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L1290-L1396)

## Overview
Determines whether to use hash-based or sort-based execution strategy for set operations (INTERSECT/EXCEPT) by comparing estimated costs and resource requirements.

## Definition

```c
static bool
choose_hashed_setop(PlannerInfo *root, List *groupClauses,
					Path *input_path,
					double dNumGroups, double dNumOutputRows,
					const char *construct)
```
## Detailed Description
This function implements a cost-based decision algorithm for choosing between hashing and sorting strategies in set operations. The decision process follows these steps:

1. **Capability Check**: Verifies whether the data types support sorting and/or hashing operations using  and 
2. **Early Decisions**: Returns immediately if only one strategy is supported, or if  is disabled (favoring sort)
3. **Memory Constraint Check**: Estimates hash table memory requirements by calculating  and compares against 
4. **Cost Comparison**: Creates dummy Path structures to estimate costs for both strategies:
   - Hash strategy: Uses  with AGG_HASHED mode
   - Sort strategy: Uses  followed by  to model sort+group execution
5. **Final Decision**: Uses  with the appropriate tuple fraction to select the cheaper alternative

The function accounts for the fact that set operation inputs are always unsorted (coming from appended sub-relations) and considers both startup and total costs in the comparison.

## Parameters / Member Variables
- : PlannerInfo containing global planning context and configuration settings
- : List of grouping clauses that define the grouping/comparison semantics for the operation
- : Path representing the input to the set operation (typically an Append path)
- : Estimated number of distinct groups expected in the operation
- : Estimated number of rows in the final output
- : String name of the operation ("INTERSECT" or "EXCEPT") for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md)
  - [grouping_is_sortable](../g/grouping_is_sortable.md)
  - [grouping_is_hashable](../g/grouping_is_hashable.md)
  - [cost_agg](cost_agg.md)
  - [cost_sort](cost_sort.md)
  - [cost_group](cost_group.md)
  - [compare_fractional_path_costs](compare_fractional_path_costs.md)
  - MAXALIGN
  - SizeofMinimalTupleHeader
  - AGG_HASHED
- Called from (representative examples):
  - [generate_nonunion_paths](../g/generate_nonunion_paths.md)

## Notes and Other Information
- The function assumes input is always unsorted since it comes from appending unrelated sub-relations
- [Hash](../H/Hash.md) entry size calculation includes both tuple width and minimal tuple header overhead with proper alignment
- When  is false, the function always chooses sorting regardless of cost estimates
- The cost comparison uses fractional path costing to properly account for LIMIT clauses and partial result retrieval
- Error reporting provides helpful details when data types have conflicting sorting/hashing support capabilities
- The function serves as a critical decision point that can significantly impact set operation performance