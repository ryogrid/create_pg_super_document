# set_join_references

## Location
src/backend/optimizer/plan/setrefs.c: 2282 - 2430

## Overview
Modifies the target list and quals of join nodes to reference their subplans by adjusting variable references to use OUTER_VAR/INNER_VAR and mapping attributes to result domain numbers.

## Definition
```c
static void set_join_references(PlannerInfo *root, Join *join, int rtoffset)
```

## Detailed Description
The set_join_references function is responsible for adjusting variable references in join nodes so they correctly reference the output columns of their subplans. This is a critical step in the plan reference fixing process that ensures join expressions can properly access data from both sides of the join.

The function performs several key operations:

1. **Target List Indexing**: Creates indexed target lists for both outer (left) and inner (right) subplans to enable efficient column lookups during reference fixing.

2. **Join Qualification Processing**: Processes join qualifications (including merge and hash clauses) first, since these are logically below the join and can access all values from input target lists.

3. **Join-Type-Specific Handling**:
   - **NestLoop**: Processes NestLoopParam expressions, ensuring they reference the outer relation correctly and handling nulling relations appropriately
   - **MergeJoin**: Fixes merge clauses that define the join conditions
   - **HashJoin**: Processes both hash clauses and hash keys, with special handling for hash keys that reference the outer plan

4. **Upper-Level Expression Processing**: Handles target list and qual expressions that are logically above the join, with special consideration for outer joins where nulling relations may be supersets rather than exact matches.

5. **Nulling Relations Handling**: Uses different nulling relation matching modes (NRM_EQUAL, NRM_SUBSET, NRM_SUPERSET) based on the context and join type to properly handle NULL-producing outer joins.

The function ensures that all variable references use the correct varno values (OUTER_VAR for left subplan, INNER_VAR for right subplan) and that attribute numbers correspond to positions in the respective target lists.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning context and information
- `join`: The Join plan node whose references need to be fixed
- `rtoffset`: Range table offset for adjusting variable numbers in nested contexts

## Dependencies
- Functions called/Symbols referenced:
  - build_tlist_index
  - fix_join_expr
  - fix_upper_expr
  - NUM_EXEC_QUAL
  - NUM_EXEC_TLIST
- Called from (representative examples):
  - fix_scan_list
  - set_plan_refs

## Notes and Other Information
- The function handles the complexity of outer joins where variables may have nulling relations that are supersets of the original relations due to the NULL-producing nature of outer joins
- NestLoopParam processing includes special handling for cases where parameterized paths may not exactly match the outer-join level where they are used
- Hash join processing distinguishes between hash clauses (join conditions) and hash keys (values used for hashtable lookups from the outer plan)
- The function includes detailed comments explaining the logical ordering of operations and why certain expressions are processed before others
- Memory management includes proper cleanup of temporary indexed target lists
- Located in src/backend/optimizer/plan/setrefs.c:2282-2430