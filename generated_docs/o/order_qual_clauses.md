# order_qual_clauses

## Location
src/backend/optimizer/plan/createplan.c: 5316 - 5322

## Overview
This function sorts a list of qualification clauses into the optimal order for runtime evaluation, prioritizing security levels and execution costs.

## Definition


## Detailed Description
 is a critical optimization function in PostgreSQL's query planner that determines the order in which qualification clauses (WHERE conditions) should be evaluated at runtime. The function implements a sophisticated sorting algorithm that considers both security barriers and execution costs to maximize query performance while maintaining security guarantees.

The function handles two primary considerations:
1. **Security Level Ordering**: When security barrier quals are present, clauses with lower security levels must be evaluated before those with higher security levels. However, leakproof functions that are reasonably cheap (less than 10X cpu_operator_cost) can be promoted to security level 0.
2. **Cost-Based Ordering**: Within the same security level, clauses are ordered by estimated per-tuple execution cost, with cheaper clauses evaluated first.

The implementation uses a stable insertion sort rather than qsort() to preserve the original order when costs are identical, which maintains predictable behavior. The function works optimally with RestrictInfo nodes since they cache cost information, but can also handle bare clause nodes (though without security considerations in that case).

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and cost parameters
- : List of qualification clauses to be ordered (can be RestrictInfos or bare clauses)

### Internal QualItem Structure:
- : The qualification clause (Node pointer)
- : Per-tuple execution cost estimate
- : Security level of the clause (0 for bare clauses or leakproof functions)

## Dependencies
- Functions called/Symbols referenced:
  - list_length
  - [palloc](../p/palloc.md)
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md)
  - lfirst
  - IsA
  - lappend
  - NIL
  - cpu_operator_cost
- Called from (representative examples):
  - [get_gating_quals](../g/get_gating_quals.md)
  - [create_group_result_plan](../c/create_group_result_plan.md)
  - [create_seqscan_plan](../c/create_seqscan_plan.md)
  - [create_indexscan_plan](../c/create_indexscan_plan.md)
  - [create_bitmap_scan_plan](../c/create_bitmap_scan_plan.md)
  - [create_nestloop_plan](../c/create_nestloop_plan.md)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md)
  - Various other plan creation functions

## Notes and Other Information
- **Performance Optimization**: The function includes an early return for lists with 0 or 1 clauses to avoid unnecessary overhead
- **Security Considerations**: Leakproof functions under the cost threshold (10X cpu_operator_cost) are treated as security level 0, allowing them to be evaluated early for better performance
- **Stable Sorting**: Uses insertion sort instead of qsort() to maintain deterministic behavior when costs are equal
- **Memory Management**: Temporarily allocates a QualItem array for efficient sorting, then converts back to a List
- **Caching Benefits**: Works most efficiently with RestrictInfo nodes that have pre-cached cost information
- **Usage Context**: This function is called during plan creation for virtually all scan and join plan types, making it a performance-critical component of the query planner

The function represents a balance between execution efficiency and security requirements, ensuring that PostgreSQL can optimize query performance while maintaining row-level security and other security barrier constraints.