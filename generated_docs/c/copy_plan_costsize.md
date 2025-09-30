# copy_plan_costsize

## Location
[src/backend/optimizer/plan/createplan.c:5425-5446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5425-L5446)

## Overview
Copies cost and size information from a lower plan node to an inserted node, with specific handling for parallel execution flags appropriate for intermediate nodes.

## Definition
```c
static void copy_plan_costsize(Plan *dest, Plan *src)
```

## Detailed Description
This function transfers execution cost and size estimates from one Plan node to another, typically used when inserting intermediate nodes (like projection or gating nodes) into an existing plan tree. Unlike copy_generic_path_info which copies from Path to Plan, this function operates between Plan nodes.

The function copies the basic execution metrics (startup_cost, total_cost, plan_rows, plan_width) directly from source to destination. However, it makes specific assumptions about parallel execution characteristics: it assumes the inserted node is not parallel-aware (cannot participate in parallel execution coordination) but is parallel-safe (can be safely executed in a parallel context) if the child plan is parallel-safe.

Most callers modify the cost information after copying it, using this function as a starting point for their own cost calculations.

## Parameters / Member Variables
- `dest`: Destination Plan node to receive the copied cost and size information  
- `src`: Source Plan node containing the information to copy

## Dependencies
- Functions called/Symbols referenced:
  - (No function calls - direct field access only)
- Called from (representative examples):
  - [create_gating_plan](create_gating_plan.md)
  - [inject_projection_plan](../i/inject_projection_plan.md)
  - [create_mergejoin_plan](create_mergejoin_plan.md)
  - [create_hashjoin_plan](create_hashjoin_plan.md)

## Notes and Other Information
- Used specifically for Plan-to-Plan copying, not Path-to-Plan like copy_generic_path_info
- Makes conservative assumptions about parallel execution capabilities of inserted nodes
- The comment indicates most callers alter the copied information after calling this function
- Typically used when inserting intermediate processing nodes that don't change the fundamental data characteristics
- The parallel_aware flag is always set to false, reflecting that most inserted nodes are passive data transformers rather than parallel coordinators

## Simplified Source

```c
static void
copy_plan_costsize(Plan *dest, Plan *src)
{
    // Copy basic execution cost and size information
    dest->startup_cost = src->startup_cost;
    dest->total_cost = src->total_cost;
    dest->plan_rows = src->plan_rows;
    dest->plan_width = src->plan_width;

    // Set conservative parallel execution assumptions for inserted nodes
    dest->parallel_aware = false;  // Cannot coordinate parallel execution
    dest->parallel_safe = src->parallel_safe;  // Inherit safety from child
}
```