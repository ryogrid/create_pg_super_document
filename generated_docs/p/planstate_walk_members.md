# planstate_walk_members

## Location
src/backend/nodes/nodeFuncs.c: 4782 - 4795

## Overview
Walks through an array of PlanState nodes that represent constituent plans of composite execution nodes like ModifyTable, Append, MergeAppend, BitmapAnd, or BitmapOr.

## Definition
static bool planstate_walk_members(PlanState **planstates, int nplans, planstate_tree_walker_callback walker, void *context)

## Detailed Description
This static helper function is part of PostgreSQL's plan state tree walking mechanism. It iterates through an array of PlanState pointers and applies a walker callback function to each one. The function is specifically designed to handle the child plan states of composite execution nodes that contain multiple subsidiary plans.

The function operates within the larger context of , which provides a systematic way to traverse execution plan trees. It uses the  macro to invoke the walker callback on each plan state, allowing for uniform error handling and early termination if the walker function returns true.

## Parameters / Member Variables
- : Array of PlanState pointers representing the child plan states to walk through
- : The number of plan states in the planstates array
- : Callback function of type planstate_tree_walker_callback to be applied to each plan state
- : Opaque context pointer passed through to the walker function

## Dependencies
- Functions called/Symbols referenced:
  - PSWALK (macro that wraps the walker callback invocation)
- Called from (representative examples):
  - [planstate_tree_walker_impl](planstate_tree_walker_impl.md) (for T_Append case)
  - [planstate_tree_walker_impl](planstate_tree_walker_impl.md) (for T_MergeAppend case)
  - [planstate_tree_walker_impl](planstate_tree_walker_impl.md) (for T_BitmapAnd case)
  - [planstate_tree_walker_impl](planstate_tree_walker_impl.md) (for T_BitmapOr case)

## Notes and Other Information
- This is a static function internal to nodeFuncs.c, not exposed to external modules
- Returns true if any walker invocation returns true (indicating early termination), false otherwise
- The function follows PostgreSQL's convention of using boolean return values to signal continuation or termination of tree traversal
- The PSWALK macro provides a consistent interface for walker invocations and handles the context parameter passing
- Located in src/backend/nodes/nodeFuncs.c:4782-4795